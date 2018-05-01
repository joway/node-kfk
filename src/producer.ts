import * as _ from 'lodash'
import * as Kafka from 'node-rdkafka'
import * as bluebird from 'bluebird'

import {
  ConnectingError,
  ConnectedError,
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
  protected connected: boolean
  protected dead: boolean

  constructor(conf: any, topicConf: any = {}) {
    this.connected = false
    this.dead = false
    this.client = new Kafka.Producer(conf, topicConf)
  }

  abstract async graceulDead(): Promise<boolean>

  disconnect() {
    return new Promise((resolve, reject) => {
      return this.client.disconnect((err, data) => {
        if (err) {
          reject(new DisconnectError(err.message))
        }
        resolve(data)
      })
    })
  }

  flush(timeout: number) {
    return new Promise((resolve, reject) => {
      return this.client.flush(timeout, (err: Error) => {
        if (err) {
          reject(new ProducerFlushError(err.message))
        }
        resolve()
      })
    })
  }

  connect(metadataOptions: any = {}) {
    return new Promise((resolve, reject) => {
      if (this.connected) {
        reject(new ConnectedError('Has been connected'))
      }
      this.client.connect(metadataOptions, (err, data) => {
        if (err) {
          reject(new ConnectingError(err.message))
        }
      })
      this.client.on('ready', () => {
        this.connected = true

        const graceulDeath = async () => {
          console.log('disconnect from kafka')
          this.dead = true
          await this.graceulDead()
          await this.disconnect()
          process.exit(0)
        }
        process.on('SIGINT', graceulDeath)
        process.on('SIGQUIT', graceulDeath)
        process.on('SIGTERM', graceulDeath)

        resolve()
      })
    })
  }
}

export class KafkaProducer extends KafkaBasicProducer {
  async graceulDead() {
    await this.flush(FLUSH_TIMEOUT)
    return true
  }

  async produce(topic: string, partition: number, message: string, key?: string, timestamp?: string, opaque?: string) {
    return new Promise((resolve, reject) => {
      if (!this.connected) {
        reject(new ConnectionNotReadyError('Connection not ready'))
      }
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
