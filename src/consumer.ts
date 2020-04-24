import * as Kafka from 'node-rdkafka'
import * as _ from 'lodash'
import * as bluebird from 'bluebird'
import * as winston from 'winston'

import { KafkaMessage, TopicPartition, Options } from './types'
import { ConnectionDeadError } from './errors'

const DEFAULT_CONSUME_SIZE = 100
const DEFAULT_CONCURRENT = 100
const DEFAULT_AUTO_COMMIT_INTERVAL = 1000 // ms
const ErrorCode = Kafka.CODES.ERRORS

export abstract class KafkaBasicConsumer {
  public consumer: Kafka.KafkaConsumer
  protected logger: winston.Logger
  protected debug: boolean
  protected dying: boolean
  protected dead: boolean
  protected topics: string[]

  constructor(conf: any, topicConf: any = {}, options: Options = {}) {
    this.dying = false
    this.dead = false
    this.topics = []
    conf['auto.commit.interval.ms'] =
      conf['auto.commit.interval.ms'] || DEFAULT_AUTO_COMMIT_INTERVAL

    if (!conf['rebalance_cb']) {
      conf['rebalance_cb'] = (err: any, assignment: any) => {
        if (err.code === ErrorCode.ERR__ASSIGN_PARTITIONS) {
          this.consumer.assign(assignment)
          let rebalanceLog = 'consumer rebalance : '
          for (const assign of assignment) {
            rebalanceLog += `{topic ${assign.topic}, partition: ${assign.partition}} `
          }
          this.logger.info(rebalanceLog)
        } else if (err.code === ErrorCode.ERR__REVOKE_PARTITIONS) {
          this.consumer.unassign()
        } else {
          this.logger.error(err)
        }
      }
    }

    this.consumer = new Kafka.KafkaConsumer(conf, topicConf)

    this.debug = options.debug === undefined ? false : options.debug
    this.logger = winston.createLogger({
      level: this.debug ? 'debug' : 'info',
      format: winston.format.simple(),
      transports: [new winston.transports.Console()],
    })
    this.logger.debug(`debug mode : ${this.debug}`)
  }

  disconnect() {
    return new Promise((resolve, reject) => {
      return this.consumer.disconnect((err: Kafka.LibrdKafkaError, data: any) => {
        if (err) {
          reject(err)
        }
        this.logger.info('consumer disconnect')
        resolve(data)
      })
    })
  }

  async connect(metadataOptions: any = {}) {
    return new Promise((resolve, reject) => {
      this.consumer.connect(metadataOptions, (err: Kafka.LibrdKafkaError, data: any) => {
        if (err) {
          reject(err)
        }

        resolve(data)
      })
    })
  }

  async die() {
    this.dying = true

    // empty topics and unsubscribe them
    this.unsubscribe()
    // disconnect from brokers
    await this.disconnect()

    this.dead = true
    this.logger.info('consumer died')
  }

  async subscribe(topics: string[]) {
    this.topics = _.uniq(_.concat(topics, this.topics))
    // synchronously
    this.consumer.subscribe(this.topics)
  }

  unsubscribe() {
    this.topics.length = 0
    this.consumer.unsubscribe()
  }

  offsetsStore(topicPartitions: TopicPartition[]) {
    if (topicPartitions.length) {
      return this.consumer.offsetsStore(topicPartitions)
    }
  }

  fetch(size: number): Promise<KafkaMessage[]> {
    // This will keep going until it gets ERR__PARTITION_EOF or ERR__TIMED_OUT
    return new Promise((resolve, reject) => {
      return this.consumer.consume(size, (err: Kafka.LibrdKafkaError, messages: KafkaMessage[]) => {
        if (err) {
          return reject(err)
        }

        resolve(messages)
      })
    })
  }
}

// `at least once` Consumer
export class KafkaALOConsumer extends KafkaBasicConsumer {
  private legacyMessages: KafkaMessage[] | null

  constructor(conf: any, topicConf: any = {}, options: Options = {}) {
    conf['enable.auto.commit'] = true
    conf['enable.auto.offset.store'] = false
    super(conf, topicConf, options)

    this.legacyMessages = null
  }

  async consume(
    cb: (message: KafkaMessage) => any,
    options: { size?: number; concurrency?: number } = {},
  ): Promise<KafkaMessage[]> {
    // default option value
    options.size = options.size || DEFAULT_CONSUME_SIZE
    options.concurrency = options.concurrency || DEFAULT_CONCURRENT
    const topicPartitionMap: { [key: string]: TopicPartition } = {}

    if (this.dying || this.dead) {
      throw new ConnectionDeadError('Connection has been dead or is dying')
    }

    const messages = this.legacyMessages || (await this.fetch(options.size!))

    if (this.debug) {
      this.logger.debug(`fetched ${messages.length} messages`)
    }

    // get latest topicPartitions
    for (let i = messages.length - 1; i >= 0; i -= 1) {
      const message = messages[i]
      const key = `${message.topic}:${message.partition}`
      if (topicPartitionMap[key] === undefined) {
        topicPartitionMap[key] = {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
        }
      }
    }

    if (!cb) {
      this.offsetsStore(_.values(topicPartitionMap))
      return messages
    }

    try {
      const results = await bluebird.map(
        messages,
        async (message: KafkaMessage) => {
          const ret = await bluebird.resolve(cb(message))
          return ret
        },
        { concurrency: options.concurrency },
      )

      this.offsetsStore(_.values(topicPartitionMap))
      this.legacyMessages = null
      return results
    } catch (e) {
      this.legacyMessages = messages
      throw e
    }
  }
}

// `At Most Once` Consumer
export class KafkaAMOConsumer extends KafkaBasicConsumer {
  constructor(conf: any, topicConf: any = {}, options: Options = {}) {
    conf['enable.auto.commit'] = true
    conf['enable.auto.offset.store'] = true

    super(conf, topicConf, options)
  }

  async consume(
    cb: (message: KafkaMessage) => any,
    options: { size?: number; concurrency?: number } = {},
  ): Promise<boolean | KafkaMessage[]> {
    // default option value
    options.size = options.size || DEFAULT_CONSUME_SIZE
    options.concurrency = options.concurrency || DEFAULT_CONCURRENT

    return new Promise<KafkaMessage[]>((resolve, reject) => {
      // This will keep going until it gets ERR__PARTITION_EOF or ERR__TIMED_OUT
      return this.consumer.consume(
        options.size!,
        async (err: Kafka.LibrdKafkaError, messages: KafkaMessage[]) => {
          if (this.dying || this.dead) {
            reject(new ConnectionDeadError('Connection has been dead or is dying'))
          }
          if (err) {
            reject(err)
          }

          return bluebird
            .map(
              messages,
              async (message: KafkaMessage) => {
                return await Promise.resolve(cb(message))
              },
              { concurrency: options.concurrency! },
            )
            .then((results: any[]) => resolve(results))
        },
      )
    })
  }
}
