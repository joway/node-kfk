import * as Kafka from 'node-rdkafka'

export interface KafkaMessage extends Kafka.Message {}

export interface KafkaMessageError {
  message: KafkaMessage
  error: Error
}

export interface TopicPartition {
  topic: string
  partition: number
  offset: number
}

export interface KafkaMetadata {
  topics: {
    name: string
    partitions: {
      id: number
      leader: number
      replicas: number[]
      isrs: number[]
    }[]
  }[]
}

export interface Options {
  debug?: boolean
}
