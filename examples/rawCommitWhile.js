const _ = require('lodash')
const Kafka = require('node-rdkafka')

function random(len) {
  const possible = 'abcdefghijklmnopqrstuvwxyz'
  return _.sampleSize(possible, len).join('')
}

const main = async () => {
  console.log('start')
  const topic = 'rdkafka-test0'
  const seed = random(12)
  const consumer = new Kafka.KafkaConsumer({
    'group.id': `raw-consumer-commit-xxxx`,
    'metadata.broker.list': '127.0.0.1:9092',
    'enable.auto.offset.store': false,
    'enable.auto.commit': false,
    'rebalance_cb': function (err, assignment) {
      if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
        // Note: this can throw when you are disconnected. Take care and wrap it in
        // a try catch if that matters to you
        let rebalance_log = 'consumer rebalance : '
        for (const assign of assignment) {
          rebalance_log += `{topic ${assign.topic}, partition: ${assign.partition}} `
        }
        console.log(rebalance_log)
        this.assign(assignment)
      } else if (err.code == Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
        // Same as above
        this.unassign();
      } else {
        // We had a real error
        console.error(err);
      }
    }
  }, {
      'auto.offset.reset': 'earliest',
    })

  consumer.connect()
  consumer.on('error', function (err) {
    console.log('error')
  })
  consumer.on('ready', async function () {
    consumer.subscribe([
      topic,
    ])

    while (true) {
      await new Promise((resolve, reject) => {
        consumer.consume(1, (err, messages) => {
          if (messages.length === 0) {
            console.log('empty messages')
            return resolve()
          }
          const message = messages[0]
          console.log(`topic: ${message.topic} offset : ${message.offset} partition : ${message.partition} val: ${message.value.toString('utf-8')}`)
          console.log('hit bottom offset', messages[messages.length - 1].offset)
          consumer.commitMessageSync(message)
          resolve(message)
        })
      })
    }
  })
}

main()
