import * as _ from 'lodash'
import * as Kafka from 'node-rdkafka'
import * as bluebird from 'bluebird'

import { TopicPartition, KafkaMetadata, KafkaMessage, KafkaMessageError } from './types'
import {
  ConnectingError,
  ConnectedError,
  DisconnectError,
  ConnectionNotReadyError,
  ConnectionDeadError,
  ConsumerRuntimeError,
  MetadataError,
  SeekError,
} from './errors'

const SEEK_TIMEOUT = 1000
const ErrorCode = Kafka.CODES.ERRORS

export abstract class KafkaBasicConsumer {
  public consumer: Kafka.KafkaConsumer
  protected connected: boolean
  protected ready: boolean
  protected dead: boolean
  protected topics: string[]

  protected offsetStore: { [key: string]: { [key: number]: number } } = {}
  protected errOffsetStore: { [key: string]: { [key: number]: number } } = {}

  constructor(conf: any, topicConf: any = {}) {
    this.connected = false
    this.ready = false
    this.dead = false
    this.topics = []

    if (conf['rebalance_cb'] === undefined) {
      conf['rebalance_cb'] = (err: any, assignment: any) => {
        if (err.code === ErrorCode.ERR__ASSIGN_PARTITIONS) {
          // Note: this can throw when you are disconnected. Take care and wrap it in
          // a try catch if that matters to you
          this.consumer.assign(assignment)
          console.log(`Consumer rebalanced at : `)
          for (const assign of assignment) {
            console.log(`   topic ${assign.topic}, partition: ${assign.partition}`)
          }
        } else if (err.code == ErrorCode.ERR__REVOKE_PARTITIONS) {
          // Same as above
          this.consumer.unassign()
        } else {
          // We had a real error
          console.error(err)
        }
      }
    }
    this.consumer = new Kafka.KafkaConsumer(conf, topicConf)
  }

  abstract async graceulDead(): Promise<boolean>

  disconnect() {
    return new Promise((resolve, reject) => {
      return this.consumer.disconnect((err, data) => {
        if (err) {
          reject(new DisconnectError(err.message))
        }
        console.log('Consumer disconnect success')
        resolve(data)
      })
    })
  }

  // rebalancing is managed internally by librdkafka by default
  async connect(metadataOptions: any = {}) {
    if (this.connected) {
      throw new ConnectedError('Has been connected')
    }

    return new Promise((resolve, reject) => {
      this.consumer.connect(metadataOptions, (err, data) => {
        if (err) {
          reject(new ConnectingError(err.message))
        }
      })

      // listen connected event
      return this.consumer.on('ready', async () => {
        this.connected = true

        const graceulDeath = async () => {
          this.dead = true
          await this.graceulDead()
          await this.disconnect()

          console.log('Consumer graceul death success')
          process.exit(0)
        }
        process.on('SIGINT', graceulDeath)
        process.on('SIGQUIT', graceulDeath)
        process.on('SIGTERM', graceulDeath)

        resolve()
      })
    })
  }

  async subscribe(topics: string[]) {
    this.topics = _.uniq(_.concat(topics, this.topics))
    // synchronously
    this.consumer.subscribe(this.topics)
    // refresh offset
    await this.initOffsetStroe()
  }

  unsubscribe() {
    this.topics.length = 0
    this.consumer.unsubscribe()
  }

  getMetadata(metadataOptions: any): Promise<KafkaMetadata> {
    return new Promise((resolve, reject) => {
      this.consumer.getMetadata(metadataOptions, (err: Error, data: KafkaMetadata) => {
        if (err) {
          reject(new MetadataError(err.message))
        }
        resolve(data)
      })
    })
  }

  seek(toppar: TopicPartition, timeout: number) {
    return new Promise((resolve, reject) => {
      this.consumer.seek(toppar, timeout, (err: Error) => {
        if (err) {
          reject(new SeekError(err.message))
        }
        resolve()
      })
    })
  }

  async initOffsetStroe() {
    this.ready = true
    const meta = await this.getMetadata({ timeout: 1000 })
    for (const topic of meta.topics) {
      if (this.topics.includes(topic.name)) {
        this.offsetStore[topic.name] = {}
        this.errOffsetStore[topic.name] = {}
        for (const p of topic.partitions) {
          this.offsetStore[topic.name][p.id] = -1
          this.errOffsetStore[topic.name][p.id] = -1
        }
      }
    }
  }

  async commits() {
    for (const topic in this.offsetStore) {
      for (const partition in this.offsetStore[topic]) {
        let offset = this.offsetStore[topic][partition]
        const errOffset = this.errOffsetStore[topic][partition]
        let isNeedSeekBack = false

        if (errOffset >= 0) {
          offset = errOffset - 1
          // clear errorOffset
          this.errOffsetStore[topic][partition] = -1
          isNeedSeekBack = true
        }

        if (offset < 0) {
          continue
        }

        const toppar = {
          topic,
          partition: parseInt(partition),
          offset: offset + 1,
        }
        this.consumer.commitSync(toppar)
        if (isNeedSeekBack) {
          await this.seek(toppar, SEEK_TIMEOUT)
        }
        this.offsetStore[topic][partition] = -1
      }
    }
  }
}

// `at least once` Consumer
// You must guarantee that your consumer cb function will not throw any Error.
// Otherwise, it will to been blocked on the offset where throw Error
export class KafkaALOConsumer extends KafkaBasicConsumer {
  async graceulDead(): Promise<boolean> {
    await this.commits()
    return true
  }

  async consume(
    cb: (message: KafkaMessage) => any,
    options: { size: number, concurrency: number, } = { size: 100, concurrency: 100 },
  ): Promise<boolean> {
    // default option value
    if (!options.size) {
      options.size = 100
    }
    if (!options.concurrency) {
      options.concurrency = options.size
    }

    if (!this.connected) {
      throw new ConnectionNotReadyError('Connection not ready')
    }

    let success = true

    return new Promise<boolean>((resolve, reject) => {
      // This will keep going until it gets ERR__PARTITION_EOF or ERR__TIMED_OUT
      return this.consumer.consume(options.size, async (err: Error, messages: KafkaMessage[]) => {
        if (this.dead) {
          reject(new ConnectionDeadError('Connection has been dead or is dying'))
        }
        if (err) {
          reject(new ConsumerRuntimeError(err.message))
        }
        try {
          await bluebird.map(messages, async message => {
            // stop the topicPartition progress then has error throw
            if (this.errOffsetStore[message.topic][message.partition] >= 0) {
              return
            }
            try {
              await Promise.resolve(cb(message))
              // stop the topicPartition progress then has error throw
              if (this.errOffsetStore[message.topic][message.partition] >= 0) {
                return
              }
              // update success offset to max one
              this.offsetStore[message.topic][message.partition] = Math.max(
                this.offsetStore[message.topic][message.partition],
                message.offset,
              )
            } catch (e) {
              success = false
              // fallback to last message
              if (this.errOffsetStore[message.topic][message.partition] < 0) {
                this.errOffsetStore[message.topic][message.partition] = message.offset
              } else {
                // fallback to the smallest offset
                this.errOffsetStore[message.topic][message.partition] = Math.min(
                  this.errOffsetStore[message.topic][message.partition],
                  message.offset,
                )
              }
            }
          }, { concurrency: options.concurrency })

          await this.commits()
        } catch (e) {
          reject(new ConsumerRuntimeError(err.message))
        }

        return resolve(success)
      })
    })
  }
}

// `At Most Once` Consumer
export class KafkaAMOConsumer extends KafkaBasicConsumer {
  constructor(conf: any, topicConf: any = {}) {
    conf['enable.auto.commit'] = true
    super(conf, topicConf)
  }

  async graceulDead(): Promise<boolean> {
    return true
  }

  async subscribe(topics: string[]) {
    this.topics = _.uniq(_.concat(topics, this.topics))
    // synchronously
    this.consumer.subscribe(this.topics)
  }

  async consume(
    cb: (message: KafkaMessage) => any,
    options: { size: number, concurrency: number, } = { size: 100, concurrency: 100 },
  ): Promise<boolean> {
    // default option value
    if (!options.size) {
      options.size = 100
    }
    if (!options.concurrency) {
      options.concurrency = options.size
    }

    if (!this.connected) {
      throw new ConnectionNotReadyError('Connection not ready')
    }

    let success = true

    return new Promise<boolean>((resolve, reject) => {
      // This will keep going until it gets ERR__PARTITION_EOF or ERR__TIMED_OUT
      return this.consumer.consume(options.size, async (err: Error, messages: KafkaMessage[]) => {
        if (this.dead) {
          reject(new ConnectionDeadError('Connection has been dead or is dying'))
        }
        if (err) {
          reject(new ConsumerRuntimeError(err.message))
        }
        try {
          await bluebird.map(messages, async message => {
            try {
              await Promise.resolve(cb(message))
            } catch (e) {
              success = false
            }
          }, { concurrency: options.concurrency })
        } catch (e) {
          reject(new ConsumerRuntimeError(err.message))
        }
        return resolve(success)
      })
    })
  }
}
