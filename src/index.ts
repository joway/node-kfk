import * as Kafka from 'node-rdkafka'
import { Dictionary } from 'lodash';

export interface KafkaMessage {
  value: Buffer // message contents as a Buffer
  size: number // size of the message, in bytes
  topic: string // topic the message comes from
  offset: number // offset the message was read from
  partition: number // partition the message was on
  key: string // key of the message if present
  timestamp: number // timestamp of message creation
}

export class KafkaProducer {
  private producer: Kafka.Producer
  private ready: boolean

  constructor(conf: any, topicConf: any = {}) {
    this.ready = false
    this.producer = new Kafka.Producer(conf, topicConf)
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
        resolve()
      })
    })
  }

  async produce(topic: string, partition: number, message: string, key?: string, timestamp?: string, opaque?: string) {
    if (!this.ready) {
      throw new Error('Kafka Connection Not Ready')
    }
    return new Promise((resolve, reject) => {
      try {
        this.producer.produce(topic, partition, new Buffer(message), key, timestamp || Date.now(), opaque)
        resolve()
      } catch (err) {
        reject(err)
      }
    })
  }
}

export class KafkaBasicConsumer {
  public consumer: Kafka.KafkaConsumer
  protected ready: boolean

  constructor(conf: any, topicConf: any = {}) {
    this.ready = false
    this.consumer = new Kafka.KafkaConsumer(conf, topicConf)
  }

  // rebalancing is managed internally by librdkafka by default
  async connect(metadataOptions: any = {}) {
    return new Promise((resolve, reject) => {
      this.consumer.connect(metadataOptions, (err, data) => {
        if (err) {
          reject(err)
        }
      })
      return this.consumer.on('ready', () => {
        this.ready = true
        resolve()
      })
    })
  }

  subscribe(topics: string[]) {
    return this.consumer.subscribe(topics)
  }

  unsubscribe() {
    return this.consumer.unsubscribe()
  }
}

export class KafkaSampleConsumer extends KafkaBasicConsumer {
  flowing(cb: (err: Error, message: KafkaMessage) => void) {
    return this.consumer.consume(cb, null)
  }

  // fetch one message
  async fetch(): Promise<KafkaMessage> {
    return new Promise<KafkaMessage>((resolve, reject) => {
      return this.consumer.consume(1, (err: Error, message: KafkaMessage) => {
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
  // This will keep going until it gets ERR__PARTITION_EOF or ERR__TIMED_OUT
  async consume(
    size: number = 100,
    cb: (err: Error, message: KafkaMessage) => void,
  ): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      return this.consumer.consume(size, (err: Error, messages: KafkaMessage[]) => {
        if (err) {
          reject(err)
        }
        for (const message of messages) {
          Promise.resolve(cb(err, message))
            .then(() => {
              this.consumer.commitMessage(message)
            })
            .catch(e => {
              reject(e)
            })
        }
        return resolve(true)
      })
    })
  }
}

export default {
  KafkaProducer,
  KafkaSampleConsumer,
  KafkaALOConsumer,
}
