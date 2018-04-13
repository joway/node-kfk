# Node Rdkafka Promise

![npm](https://img.shields.io/npm/v/node-rdkafka-promise.svg)
![npm](https://img.shields.io/npm/dt/node-rdkafka-promise.svg)

The high-level node kafka client with Promise support .

## Usage

### Install

```shell
npm i node-rdkafka-promise -S
```

### Kafka Producer

```js
const conf = {
  'client.id': 'kafka',
  'metadata.broker.list': 'localhost:9092',
  'compression.codec': 'gzip',
  'retry.backoff.ms': 200,
  'message.send.max.retries': 10,
  'socket.keepalive.enable': true,
  'queue.buffering.max.messages': 1000,
  'queue.buffering.max.ms': 100,
  'batch.num.messages': 1000000,
}
const topicConf = {}
const producer = new KafkaProducer(conf, topicConf)

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
```

### Kafka Consumer

```js
const conf = {
  'group.id': 'kafka',
  'metadata.broker.list': 'localhost:9092',
  'enable.auto.commit': true, // recommend
}
const topicConf = {
  'auto.offset.reset': 'smallest',
}
const consumer = new KafkaConsumer(conf)

consumer.connect()
  .then(() => {
    consumer.subscribe(['librdtesting-01'])
  })
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
```
