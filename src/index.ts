import * as _ from 'lodash'
import * as Kafka from 'node-rdkafka'
import * as bluebird from 'bluebird'

const ErrorCode = Kafka.CODES.ERRORS

export interface KafkaMessage {
  value: Buffer // message contents as a Buffer
  size: number // size of the message, in bytes
  topic: string // topic the message comes from
  offset: number // offset the message was read from
  partition: number // partition the message was on
  key: string // key of the message if present
  timestamp: number // timestamp of message creation
}

export interface KafkaMessageError {
  message: KafkaMessage
  error: Error
}

export interface TopicPartition {
  topic: string,
  partition: number,
  offset: number,
}

export interface KafkaMetadata {
  topics: {
    name: string,
    partitions: {
      id: number,
      leader: number,
      replicas: number[],
      isrs: number[]
    }[]
  }[]
}

export class KafkaProducer {
  private producer: Kafka.Producer
  private ready: boolean
  private dead: boolean

  constructor(conf: any, topicConf: any = {}) {
    this.ready = false
    this.dead = false
    this.producer = new Kafka.Producer(conf, topicConf)
  }

  async graceulExit() {
    return this.flush(1000)
  }

  async disconnect() {
    return new Promise((resolve, reject) => {
      return this.producer.disconnect((err, data) => {
        if (err) {
          reject(err)
        }
        resolve(data)
      })
    })
  }

  async connect(metadataOptions: any = {}) {
    return new Promise((resolve, reject) => {
      this.producer.connect(metadataOptions, (err, data) => {
        if (err) {
          reject(err)
        }
      })

      return this.producer.on('ready', () => {
        this.ready = true

        const graceulDeath = async () => {
          console.log('disconnect from kafka')
          this.dead = true

          try {
            await this.graceulExit()
            await this.disconnect()
            console.log('disconnect clean success')
          } catch (e) {
            console.error(e)
          } finally {
            process.exit(0)
          }
        }
        process.on('SIGINT', graceulDeath)
        process.on('SIGQUIT', graceulDeath)
        process.on('SIGTERM', graceulDeath)

        resolve()
      })
    })
  }

  async flush(timeout: number) {
    return new Promise((resolve, reject) => {
      return this.producer.flush(timeout, (err: Error) => {
        if (err) {
          reject(err)
        }
        resolve()
      })
    })
  }

  async produce(topic: string, partition: number, message: string, key?: string, timestamp?: string, opaque?: string) {
    if (!this.ready) {
      throw new Error('Kafka Connection Not Ready')
    }
    if (this.dead) {
      throw new Error('Kafka Producer is dead')
    }
    return new Promise((resolve, reject) => {
      try {
        // synchronously
        this.producer.produce(topic, partition, new Buffer(message), key, timestamp || Date.now(), opaque)
        resolve()
      } catch (err) {
        if (err.code === ErrorCode.ERR__QUEUE_FULL) {
          return this.flush(100)
            .then(() => {
              resolve()
            })
        }
        reject(err)
      }
    })
  }
}

export abstract class KafkaBasicConsumer {
  public consumer: Kafka.KafkaConsumer
  protected ready: boolean
  protected dead: boolean
  protected topics: string[]

  protected offsetStore: { [key: string]: { [key: number]: number } } = {}
  protected errorOffsetStore: { [key: string]: { [key: number]: number } } = {}

  constructor(conf: any, topicConf: any = {}) {
    this.ready = false
    this.dead = false
    this.topics = []
    this.consumer = new Kafka.KafkaConsumer(conf, topicConf)
  }

  abstract async graceulExit(): Promise<boolean>

  async disconnect() {
    return new Promise((resolve, reject) => {
      return this.consumer.disconnect((err, data) => {
        if (err) {
          reject(err)
        }
        resolve(data)
      })
    })
  }

  // rebalancing is managed internally by librdkafka by default
  async connect(metadataOptions: any = {}) {
    return new Promise((resolve, reject) => {
      this.consumer.connect(metadataOptions, (err, data) => {
        if (err) {
          reject(err)
        }
      })

      return this.consumer.on('ready', async () => {
        this.ready = true

        const graceulDeath = async () => {
          console.log('disconnect from kafka')
          this.dead = true

          try {
            await this.graceulExit()
            await this.disconnect()
            console.log('disconnect clean success')
          } catch (e) {
            console.error(e)
          } finally {
            process.exit(0)
          }
        }
        process.on('SIGINT', graceulDeath)
        process.on('SIGQUIT', graceulDeath)
        process.on('SIGTERM', graceulDeath)

        resolve()
      })
    })
  }

  async prepare() {
    const meta = await this.getMetadata({ timeout: 1000 })

    meta.topics.forEach((topic: { name: string, partitions: any[] }) => {
      if (!this.topics.includes(topic.name)) {
        return
      }

      this.offsetStore[topic.name] = {}
      topic.partitions.forEach(p => {
        this.offsetStore[topic.name][p.id] = -1
      })
    })
    meta.topics.forEach((topic: { name: string, partitions: any[] }) => {
      this.errorOffsetStore[topic.name] = {}
      topic.partitions.forEach(p => {
        this.errorOffsetStore[topic.name][p.id] = -1
      })
    })
  }

  subscribe(topics: string[]) {
    this.topics = _.uniq(_.concat(topics, this.topics))
    return this.consumer.subscribe(topics)
  }

  unsubscribe() {
    this.topics = []
    return this.consumer.unsubscribe()
  }

  getMetadata(metadataOptions: any): Promise<KafkaMetadata> {
    return new Promise((resolve, reject) => {
      this.consumer.getMetadata(metadataOptions, (err: Error, data: KafkaMetadata) => {
        if (err) {
          reject(err)
        }
        resolve(data)
      })
    })
  }

  seek(toppar: TopicPartition, timeout: number) {
    return new Promise((resolve, reject) => {
      this.consumer.seek(toppar, timeout, (err: Error) => {
        if (err) {
          reject(err)
        }

        resolve()
      })
    })
  }
}

export class KafkaSampleConsumer extends KafkaBasicConsumer {
  async graceulExit(): Promise<boolean> {
    return true
  }

  // register message callback function
  flowing(cb: (err: Error, message: KafkaMessage) => void) {
    if (!this.ready) {
      throw new Error('Kafka Connection Not Ready')
    }
    if (this.dead) {
      throw new Error('Kafka Consumer Dead')
    }
    return this.consumer.consume(cb, null)
  }

  // fetch one message
  async fetch(): Promise<KafkaMessage> {
    if (!this.ready) {
      throw new Error('Kafka Connection Not Ready')
    }
    if (this.dead) {
      throw new Error('Kafka Consumer Dead')
    }
    return new Promise<KafkaMessage>((resolve, reject) => {
      return this.consumer.consume(1, (err: Error, message: KafkaMessage) => {
        if (this.dead) {
          reject(new Error('Kafka Consumer Dead'))
        }
        if (err) {
          reject(err)
        }
        return resolve(message)
      })
    })
  }
}

// Kafka `at least once` Consumer
export class KafkaALOConsumer extends KafkaBasicConsumer {
  async graceulExit(): Promise<boolean> {
    try {
      await this.commitAll()
      return true
    } catch (err) {
      return false
    }
  }

  async commitAll() {
    for (const topic in this.offsetStore) {
      for (const partition in this.offsetStore[topic]) {
        let offset = this.offsetStore[topic][partition]
        const errOffset = this.errorOffsetStore[topic][partition]

        if (errOffset >= 0) {
          offset = errOffset - 1
          // clear errorOffset
          this.errorOffsetStore[topic][partition] = -1
        }

        if (offset < 0) {
          continue
        }

        const toppar = {
          topic,
          partition: parseInt(partition),
          offset: offset + 1,
        }

        console.log('commit', JSON.stringify(toppar))
        this.consumer.commitSync(toppar)
        await this.seek(toppar, 100)
        this.offsetStore[topic][partition] = -1
      }
    }
  }

  async consume(
    cb: (message: KafkaMessage) => any,
    options: { size: number, concurrency: number, } = { size: 100, concurrency: 100 },
  ): Promise<KafkaMessageError[]> {
    // default option value
    if (!options.size) {
      options.size = 100
    }
    if (!options.concurrency) {
      options.concurrency = options.size
    }

    if (this.dead) {
      return []
    }

    return new Promise<KafkaMessageError[]>((resolve, reject) => {
      // This will keep going until it gets ERR__PARTITION_EOF or ERR__TIMED_OUT
      return this.consumer.consume(options.size, async (err: Error, messages: KafkaMessage[]) => {
        if (this.dead) {
          return
        }
        if (err) {
          reject(err)
        }
        const errs: KafkaMessageError[] = []
        await bluebird.map(messages, async message => {
          if (this.errorOffsetStore[message.topic][message.partition] >= 0) {
            return
          }
          try {
            await Promise.resolve(cb(message))
            if (this.errorOffsetStore[message.topic][message.partition] >= 0) {
              return
            }
            this.offsetStore[message.topic][message.partition] = Math.max(
              this.offsetStore[message.topic][message.partition],
              message.offset,
            )
          } catch (e) {
            // fallback to last message
            if (this.errorOffsetStore[message.topic][message.partition] < 0) {
              this.errorOffsetStore[message.topic][message.partition] = message.offset
            } else {
              this.errorOffsetStore[message.topic][message.partition] = Math.min(
                this.errorOffsetStore[message.topic][message.partition],
                message.offset,
              )
            }
            if (this.errorOffsetStore[message.topic][message.partition] < 0) {
              this.errorOffsetStore[message.topic][message.partition] = 0
            }
            errs.push({
              message,
              error: e,
            })
          }
        }, { concurrency: options.concurrency })

        await this.commitAll()
        return resolve(errs)
      })
    })
  }
}

export default {
  KafkaProducer,
  KafkaSampleConsumer,
  KafkaALOConsumer,
}
