const KafkaProducer = require('./dist/src').KafkaProducer
const KafkaConsumer = require('./dist/src').KafkaConsumer

const producer = new KafkaProducer({
  'client.id': 'kafka',
  'metadata.broker.list': 'localhost:9092',
  'compression.codec': 'gzip',
  'retry.backoff.ms': 200,
  'message.send.max.retries': 10,
  'socket.keepalive.enable': true,
  'queue.buffering.max.messages': 100000,
  'queue.buffering.max.ms': 1000,
  'batch.num.messages': 1000000,
  'dr_cb': true
}, {})

producer.connect()
  .then(() => {
    producer.produce('topicName', -1, 'some message')
      .catch(err => {
        console.error(err)
      })
  })
  .catch(err => {
    console.error(err)
  })

const consumer = new KafkaConsumer({
  'group.id': 'kafka',
  'metadata.broker.list': 'localhost:9092',
})

consumer.connect()
  .then(async () => {
    // fetch mode : get one message
    const message = await consumer.fetch()

    // flowing mode : registed callback function , it will be executed once get the new message
    consumer.flowing((err, message) => {
      if (err) {
        console.error(err)
        return
      }
      console.log(message)
    })
  })
