import * as Kafka from 'node-rdkafka'
import * as winston from 'winston'

import { ConnectionDeadError } from './errors'
import { Options } from './types'

const ErrorCode = Kafka.CODES.ERRORS
const FLUSH_TIMEOUT = 10000 // ms

export abstract class KafkaBasicProducer {
  public client: Kafka.Producer
  protected logger: winston.Logger
  protected debug: boolean
  protected dying: boolean
  protected dead: boolean
  protected flushing: boolean

  constructor(conf: any, topicConf: any = {}, options: Options = {}) {
    this.dying = false
    this.dead = false
    this.flushing = false
    this.client = new Kafka.Producer(conf, topicConf)

    this.debug = options.debug === undefined ? false : options.debug
    this.logger = winston.createLogger({
      level: this.debug ? 'debug' : 'info',
      format: winston.format.simple(),
      transports: [new winston.transports.Console()],
    })
  }

  disconnect() {
    return new Promise((resolve, reject) => {
      return this.client.disconnect((err: Error, data: any) => {
        if (err) {
          reject(err)
        }
        this.logger.info('producer disconnect')
        resolve(data)
      })
    })
  }

  async flush(timeout: number = FLUSH_TIMEOUT) {
    if (this.flushing) {
      return
    }
    this.flushing = true
    return new Promise((resolve, reject) => {
      return this.client.flush(timeout, (err: Kafka.LibrdKafkaError) => {
        this.flushing = false
        if (err) {
          reject(err)
        }
        resolve()
      })
    })
  }

  connect(metadataOptions: any = {}) {
    return new Promise((resolve, reject) => {
      this.client.connect(metadataOptions, (err: Kafka.LibrdKafkaError, data: any) => {
        if (err) {
          reject(err)
        }
        resolve(data)
      })
    })
  }

  async die() {
    this.dying = true
    await this.disconnect()
    this.dead = true
    this.logger.info('producer graceful died')
  }
}

export class KafkaProducer extends KafkaBasicProducer {
  async gracefulDead() {
    await this.flush(FLUSH_TIMEOUT)
    return true
  }

  async produce(
    topic: string,
    partition: number | null,
    message: string,
    key?: string,
    timestamp?: number,
    opaque?: string,
  ) {
    return new Promise((resolve, reject) => {
      if (this.dying || this.dead) {
        reject(new ConnectionDeadError('Connection has been dead or is dying'))
      }
      try {
        // synchronously
        this.client.produce(
          topic,
          partition,
          Buffer.from(message),
          key,
          timestamp || Date.now(),
          opaque,
        )
        resolve()
      } catch (err) {
        if (err.code === ErrorCode.ERR__QUEUE_FULL) {
          // flush all queued messages
          return this.flush(FLUSH_TIMEOUT).then(() => {
            resolve()
          })
        }
        reject(err)
      }
    })
  }
}
