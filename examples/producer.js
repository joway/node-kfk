const _ = require('lodash')
const crypto = require('crypto')
const bluebird = require('bluebird')
const KafkaProducer = require('../dist/src').KafkaProducer
const KfkErrorCode = require('../dist/src').KfkErrorCode

const main = async () => {
  console.log('start')

  const producer = new KafkaProducer({
    'client.id': 'kafka',
    'metadata.broker.list': '127.0.0.1:9092',
    'compression.codec': 'gzip',
    'socket.keepalive.enable': true,
    'queue.buffering.max.messages': 100,
    'queue.buffering.max.ms': 100,
    'batch.num.messages': 100000,
  }, {})

  await producer.connect()

  console.log('connected')

  while (true) {
    const topic = _.sample([
      'rdkafka-test0',
      // 'rdkafka-test1',
      // 'rdkafka-demo-1',
    ])
    const msg = `${new Date().getTime()}-${crypto.randomBytes(20).toString('hex')}`
    console.log(`create msg on topic ${topic} : ${msg}`)

    await producer.produce(topic, null, msg)

    await bluebird.delay(1)
  }
}

main()
