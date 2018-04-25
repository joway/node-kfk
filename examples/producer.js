const _ = require('lodash')
const crypto = require('crypto')
const bluebird = require('bluebird')
const KafkaProducer = require('../dist/src').KafkaProducer

const main = async () => {
  console.log('start')

  const producer = new KafkaProducer({
    'client.id': 'kafka',
    'metadata.broker.list': '127.0.0.1:9092',
    'compression.codec': 'gzip',
    'socket.keepalive.enable': true,
    'queue.buffering.max.messages': 10000,
    'queue.buffering.max.ms': 100,
    'batch.num.messages': 100000,
  }, {})

  await producer.connect()

  while (true) {
    const msg = `${new Date().getTime()}-${crypto.randomBytes(20).toString('hex')}`
    console.log(msg)

    try {
      await producer.produce(_.sample([
        'rdkafka-test0',
        'rdkafka-test1',
        'rdkafka-test2',
      ]), null, msg)
    } catch (err) {
      console.log(err)
    }
    await bluebird.delay(100)
  }
}

main()
