
const crypto = require('crypto')
const bluebird = require('bluebird')
const KafkaMessage = require('../dist/src').KafkaMessage
const KafkaALOConsumer = require('../dist/src').KafkaALOConsumer

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
  await consumer.prepare()

  while (true) {
    console.log('--------------------')
    const totalBox = new Set()
    const successBox = new Set()

    try {
      const msgErrs = await consumer.consume(message => {
        totalBox.add(message.offset)
        console.log(`topic: ${message.topic} offset : ${message.offset} val: ${message.value.toString('utf-8')}`)

        if (totalBox.size === 88 || totalBox.size === 99) {
          throw Error(`Error at : ${message.topic} ${message.partition} ${message.offset}`)
        }

        successBox.add(message.offset)
      }, {
          size: 10,
          // concurrency: 5,
        })


      if (msgErrs.length === 0) {
        continue
      }

      if ((totalBox.length - successBox.length) === msgErrs.length) {
        throw Error('assert error')
      }

      msgErrs.forEach(err => {
        console.log(err.error.toString())
      })

      // await bluebird.delay(1)
    } catch (err) {
      console.log(err)
    }
  }
}

main()
