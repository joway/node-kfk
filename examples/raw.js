
const Kafka = require('node-rdkafka')

const main = async () => {
  console.log('start')

  const consumer = new Kafka.KafkaConsumer({
    'group.id': 'raw-consumer-demo-5',
    'metadata.broker.list': '127.0.0.1:9092',
    'enable.auto.offset.store': false,
    'enable.auto.commit': false,
  }, {
      'auto.offset.reset': 'latest',
    })

  let count = 0
  consumer.connect()
  consumer.on('ready', function () {
    consumer.subscribe([
      // 'rdkafka-test0',
      // 'rdkafka-test1',
      // 'rdkafka-test2',
      'rdkafka-demo-1',
    ])

    consumer.consume()
  })
    .on('data', (message) => {
      if (count === 0) {
        console.log(`topic: ${message.topic} offset : ${message.offset} val: ${message.value.toString('utf-8')}`)
      }
      count++
    }).on('error', (err) => {
      console.log(err)
    })
}

main()
