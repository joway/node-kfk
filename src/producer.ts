import * as Kafka from 'node-rdkafka'
import * as winston from 'winston'

import {
  ConnectingError,
  DisconnectError,
  ConnectionDeadError,
  ProducerFlushError,
  ProducerRuntimeError,
} from './errors'
import { Options } from './types';

const ErrorCode = Kafka.CODES.ERRORS
const FLUSH_TIMEOUT = 10000 // ms

export abstract class KafkaBasicProducer {
  public client: Kafka.Producer
  protected logger: winston.Logger
  protected debug: boolean
  protected dying: boolean
  protected flushing: boolean

  constructor(conf: any, topicConf: any = {}, options: Options = {}) {
    this.dying = false
    this.flushing = false
    this.client = new Kafka.Producer(conf, topicConf)

    this.debug = options.debug === undefined ? false : options.debug
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
      return this.client.disconnect((err, data) => {
        if (err) {
          reject(new DisconnectError(err.message))
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
      return this.client.flush(timeout, (err: Error) => {
        this.flushing = false
        if (err) {
          reject(new ProducerFlushError(err.message))
        }
        resolve()
      })
    })
  }

  connect(metadataOptions: any = {}) {
    return new Promise((resolve, reject) => {
      this.client.connect(
        metadataOptions,
        (err, data) => {
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
      // cleanup
      await this.gracefulDead()
      await this.disconnect()

      this.logger.info('producer graceful died')
      process.exit(0)
    }
    process.on('SIGINT', gracefulDeath)
    process.on('SIGQUIT', gracefulDeath)
    process.on('SIGTERM', gracefulDeath)
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
    timestamp?: string,
    opaque?: string,
  ) {
    return new Promise((resolve, reject) => {
      if (this.dying) {
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
        reject(new ProducerRuntimeError(err.message))
      }
    })
  }
}
