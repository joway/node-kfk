
const crypto = require('crypto')
const bluebird = require('bluebird')
const KafkaALOConsumer = require('../dist/src').KafkaALOConsumer
const KfkErrorCode = require('../dist/src').KfkErrorCode

const main = async () => {
  console.log('start')

  const consumer = new KafkaALOConsumer({
    'group.id': 'alo-consumer-test-1',
    'metadata.broker.list': '127.0.0.1:9092',
    'auto.offset.reset': 'largest',
    'enable.auto.offset.store': true,
    'enable.auto.commit': false,
  }, {})
  await consumer.connect()
  await consumer.subscribe([
    'rdkafka-test0',
    'rdkafka-test1',
    'rdkafka-test2',
  ])

  while (true) {
    await consumer.consume(message => {
      console.log(`topic: ${message.topic} offset : ${message.offset} val: ${message.value.toString('utf-8')}`)
    }, {
        size: 10,
        concurrency: 5,
      })
  }
}

main()
