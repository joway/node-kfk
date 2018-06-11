import * as _ from 'lodash'
import * as Kafka from 'node-rdkafka'
import * as bluebird from 'bluebird'

import {
  ConnectingError,
  DisconnectError,
  ConnectionNotReadyError,
  ConnectionDeadError,
  ProducerFlushError,
  ProducerRuntimeError,
} from './errors'

const ErrorCode = Kafka.CODES.ERRORS
const FLUSH_TIMEOUT = 1000 // ms

export abstract class KafkaBasicProducer {
  public client: Kafka.Producer
  protected dead: boolean
  protected flushing: boolean

  constructor(conf: any, topicConf: any = {}) {
    this.dead = false
    this.flushing = false
    this.client = new Kafka.Producer(conf, topicConf)

    this.setGracefulDeath()
  }

 abstract async gracefulDead(): Promise<boolean>

  disconnect() {
    return new Promise((resolve, reject) => {
      return this.client.disconnect((err, data) => {
        if (err) {
          reject(new DisconnectError(err.message))
        }
        console.log('Producer disconnect success')
        resolve(data)
      })
    })
  }

  async flush(timeout: number) {
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
      this.client.connect(metadataOptions, (err, data) => {
        if (err) {
          reject(new ConnectingError(err.message))
        }
        resolve(data)
      })
    })
  }

  private setGracefulDeath() {
    const _gracefulDeath = async () => {
      console.log('Producer graceful death begin')

      this.dead = true
      await this.gracefulDead()
      await this.disconnect()

      console.log('Producer graceful death success')
      process.exit(0)
    }
    process.on('SIGINT', _gracefulDeath)
    process.on('SIGQUIT', _gracefulDeath)
    process.on('SIGTERM', _gracefulDeath)
  }
}

export class KafkaProducer extends KafkaBasicProducer {
  async gracefulDead() {
    await this.flush(FLUSH_TIMEOUT)
    return true
  }

  async produce(topic: string, partition: number, message: string, key?: string, timestamp?: string, opaque?: string) {
    return new Promise((resolve, reject) => {
      if (this.dead) {
        reject(new ConnectionDeadError('Connection has been dead or is dying'))
      }
      try {
        // synchronously
        this.client.produce(topic, partition, new Buffer(message), key, timestamp || Date.now(), opaque)
        resolve()
      } catch (err) {
        if (err.code === ErrorCode.ERR__QUEUE_FULL) {
          // flush all queued messages
          return this.flush(FLUSH_TIMEOUT)
            .then(() => {
              resolve()
            })
        }
        reject(new ProducerRuntimeError(err.message))
      }
    })
  }
}
