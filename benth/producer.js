const _ = require('lodash')
const crypto = require('crypto')
const bluebird = require('bluebird')
const KafkaProducer = require('../dist/src').KafkaProducer

const main = async () => {
  const producer = new KafkaProducer({
    'client.id': 'kafka',
    'metadata.broker.list': '127.0.0.1:9092',
    'retry.backoff.ms': 200,
    'compression.codec': 'gzip',
    'message.send.max.retries': 10,
    'socket.keepalive.enable': true,
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.ms': 1000,
    'batch.num.messages': 1000,
  }, {})

  await producer.connect()

  let total = 0
  const beg = new Date()

  while (true) {
    if (total >= 100000) {
      break
    }
    const msg = `${new Date().getTime()}`
    producer.produce(_.sample([
      'rdkafka-test0',
      'rdkafka-test1',
      'rdkafka-test2',
    ]), null, msg)
      .then(
        () => { total += 1 }
      )
      .catch(err => {
        // console.log(err)
      })
    
    await bluebird.delay(10)
  }
  console.log(`total messages : ${total}`)
  const end = new Date()
  const ms = end.getTime() - beg.getTime()
  console.log(`elapsed time : ${ms} ms`)
  const speed = total / ms // messages / ms

  console.log(`speed : ${speed * 1000} messages / s `)
}

main().then(() => {
  process.exit(0)
})

// === Result ===
// total messages : 100000
// elapsed time : 362 ms
// speed : 276243.09392265195 messages / s
