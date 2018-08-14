import * as Kafka from 'node-rdkafka'
import * as _ from 'lodash'
import * as bluebird from 'bluebird'
import * as assert from 'assert'
import * as winston from 'winston'

import { TopicPartition, KafkaMetadata, KafkaMessage, Options } from './types'
import {
  ConnectingError,
  DisconnectError,
  ConnectionDeadError,
  ConsumerRuntimeError,
  MetadataError,
  SeekError,
} from './errors'

const DEFAULT_CONSUME_SIZE = 100
const DEFAULT_CONCURRENT = 100
const DEFAULT_SEEK_TIMEOUT = 1000
const ErrorCode = Kafka.CODES.ERRORS

type Store = { [key: string]: { [key: string]: number } }

const setIfNotExist = (conf: any, key: string, value: any) => {
  if (conf[key] === undefined) {
    conf[key] = value
    return true
  }
  return false
}

export abstract class KafkaBasicConsumer {
  public consumer: Kafka.KafkaConsumer
  protected logger: winston.Logger
  protected debug: boolean
  protected dying: boolean
  protected topics: string[]
  /**
   * the offsets to commit :
   * - <= 0 : error offset to seek fallback
   * - > 0 : success offset to commit
   */
  protected offsetStore: Store = {}

  constructor(conf: any, topicConf: any = {}, options: Options = {}) {
    this.dying = false
    this.topics = []

    setIfNotExist(conf, 'rebalance_cb', (err: any, assignment: any) => {
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
    })

    this.consumer = new Kafka.KafkaConsumer(conf, topicConf)

    this.debug = options.debug === undefined ? false : options.debug

    if (this.debug) { console.log(`debug mode : ${this.debug}`) }

    this.logger = winston.createLogger({
      level: this.debug ? 'debug' : 'info',
      format: winston.format.simple(),
      transports: [
        new winston.transports.Console(),
      ],
    })

    this.setGracefulDeath()
  }

  abstract async gracefulDead(): Promise<boolean>

  disconnect() {
    return new Promise((resolve, reject) => {
      return this.consumer.disconnect((err: Error, data: any) => {
        if (err) {
          reject(new DisconnectError(err.message))
        }
        this.logger.info('consumer disconnect')
        resolve(data)
      })
    })
  }

  async connect(metadataOptions: any = {}) {
    return new Promise((resolve, reject) => {
      this.consumer.connect(
        metadataOptions,
        (err: Error, data: any) => {
          if (err) {
            reject(new ConnectingError(err.message))
          }

          resolve(data)
        },
      )
    })
  }

  private setGracefulDeath() {
    const gracefulDeath = async () => {
      this.dying = true
      await this.gracefulDead()
      await this.disconnect()

      this.logger.info('consumer graceful died')
      process.exit(0)
    }
    process.on('SIGINT', gracefulDeath)
    process.on('SIGQUIT', gracefulDeath)
    process.on('SIGTERM', gracefulDeath)
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

  seek(toppar: TopicPartition, timeout: number) {
    if (this.debug) { this.logger.debug(`seek ${JSON.stringify(toppar)}`) }

    return new Promise((resolve, reject) => {
      this.consumer.seek(toppar, timeout, (err: Error) => {
        if (err) {
          reject(new SeekError(err.message))
        }
        this.logger.debug('seek finished')
        resolve()
      })
    })
  }
}

// `at least once` Consumer
// You must guarantee that your consumer cb function will not throw any Error.
// Otherwise, it will to been blocked on the offset where throw Error
export class KafkaALOConsumer extends KafkaBasicConsumer {
  constructor(conf: any, topicConf: any = {}, options: Options = {}) {
    setIfNotExist(conf, 'enable.auto.commit', false)
    setIfNotExist(conf, 'enable.auto.offset.store', false)

    super(conf, topicConf, options)
  }

  private setOffset(topic: string, partition: number, offset: number) {
    if (!this.offsetStore[topic]) {
      this.offsetStore[topic] = {}
    }
    const curOffset = this.offsetStore[topic][partition]
    if (offset <= 0) {
      if (curOffset === undefined || curOffset > 0) {
        this.offsetStore[topic][partition] = offset
      } else {
        this.offsetStore[topic][partition] = Math.max(curOffset, offset)
      }
    } else {
      if (curOffset > 0) {
        this.offsetStore[topic][partition] = Math.max(curOffset, offset)
      }
    }
  }

  async gracefulDead(): Promise<boolean> {
    await this.commits()
    return true
  }

  async getMetadata(metadataOptions: any) {
    return new Promise<KafkaMetadata>((resolve, reject) => {
      this.consumer.getMetadata(metadataOptions, (err: Error, data: KafkaMetadata) => {
        if (err) {
          reject(new MetadataError(err.message))
        }
        resolve(data)
      })
    })
  }

  async commits() {
    const offsetStore = this.offsetStore
    if (this.debug) { this.logger.debug(`offsetStore ${JSON.stringify(offsetStore)}`) }

    const topics = _.keys(offsetStore)
    for (const topic of topics) {
      const partitions = _.keys(this.offsetStore[topic])
      for (const partition of partitions) {
        const offset = this.offsetStore[topic][partition]
        const fallback = offset <= 0
        const toppar = {
          topic,
          partition: parseInt(partition, 10),
          offset: fallback ? -offset : offset,
        }
        const topparStr = JSON.stringify(toppar)

        this.logger.debug(`committing topicPartition ${topparStr}`)

        if (fallback) {
          await this.seek(toppar, DEFAULT_SEEK_TIMEOUT)
          this.logger.info(`fallback seek to topicPartition: ${topparStr}`)
        } else {
          this.consumer.commitSync(toppar)

          this.logger.debug(`committed topicPartition: ${topparStr}`)
        }
        delete this.offsetStore[topic][partition]
      }
    }
  }

  async consume(
    cb: (message: KafkaMessage) => any,
    options: { size?: number; concurrency?: number } = {},
  ): Promise<KafkaMessage[]> {
    // default option value
    setIfNotExist(options, 'size', DEFAULT_CONSUME_SIZE)
    setIfNotExist(options, 'concurrency', options.size)

    return new Promise<KafkaMessage[]>((resolve, reject) => {
      // This will keep going until it gets ERR__PARTITION_EOF or ERR__TIMED_OUT
      return this.consumer.consume(options.size!, async (err: Error, messages: KafkaMessage[]) => {
        if (this.dying) {
          return reject(new ConnectionDeadError('Connection has been dead or is dying'))
        }
        if (err) {
          return reject(new ConsumerRuntimeError(err.message))
        }
        if (this.debug) { this.logger.debug(`fetch messages ${JSON.stringify(messages)}`) }

        try {
          const results = await bluebird.map(
            messages,
            async (message) => {
              try {
                const ret = await bluebird.resolve(cb(message))
                this.setOffset(message.topic, message.partition, message.offset + 1)
                return ret
              } catch (e) {
                this.setOffset(message.topic, message.partition, -message.offset)
                throw e
              }
            },
            { concurrency: options.concurrency || DEFAULT_CONCURRENT },
          )
          await this.commits()
          return resolve(results)
        } catch (e) {
          await this.commits()
          return reject(new ConsumerRuntimeError(e.message))
        }
      })
    })
  }
}

// `At Most Once` Consumer
export class KafkaAMOConsumer extends KafkaBasicConsumer {
  constructor(conf: any, topicConf: any = {}, options: Options = {}) {
    setIfNotExist(conf, 'enable.auto.commit', true)
    setIfNotExist(conf, 'enable.auto.offset.store', true)

    super(conf, topicConf, options)
  }

  async gracefulDead(): Promise<boolean> {
    return true
  }

  async consume(
    cb: (message: KafkaMessage) => any,
    options: { size?: number; concurrency?: number } = {},
  ): Promise<boolean | KafkaMessage[]> {
    // default option value
    setIfNotExist(options, 'size', DEFAULT_CONSUME_SIZE)
    setIfNotExist(options, 'concurrency', options.size)

    return new Promise<KafkaMessage[]>((resolve, reject) => {
      // This will keep going until it gets ERR__PARTITION_EOF or ERR__TIMED_OUT
      return this.consumer.consume(options.size!, async (err: Error, messages: KafkaMessage[]) => {
        if (this.dying) {
          reject(new ConnectionDeadError('Connection has been dead or is dying'))
        }
        if (err) {
          reject(new ConsumerRuntimeError(err.message))
        }

        return bluebird.map(
          messages,
          async (message) => {
            return await Promise.resolve(cb(message))
          },
          { concurrency: options.concurrency || DEFAULT_CONCURRENT },
        )
          .then((results: any[]) => (resolve(results)))
          .catch(e => (reject(new ConsumerRuntimeError(e.message))))
      })
    })
  }
}
