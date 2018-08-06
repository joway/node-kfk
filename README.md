# Node-Kfk

[![Build Status](https://travis-ci.org/joway/node-kfk.svg?branch=master)](https://travis-ci.org/joway/node-kfk)
[![npm](https://img.shields.io/npm/v/kfk.svg)](https://www.npmjs.com/package/kfk)
[![npm](https://img.shields.io/npm/dt/kfk.svg)](https://www.npmjs.com/package/kfk)

The high-level node kafka client based on node-rdkafka .

## Usage

### Install

```shell
npm i kfk -S
```

### Kafka Producer

```js
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
  const msg = `${new Date().getTime()}-${crypto.randomBytes(20).toString('hex')}`

  await producer.produce(_.sample([
    'rdkafka-test0',
    'rdkafka-test1',
    'rdkafka-test2',
  ]), null, msg)
}
```

### Kafka ALO(at least once) Consumer

```js
const consumer = new KafkaALOConsumer({
  'group.id': 'alo-consumer-test-1',
  'metadata.broker.list': '127.0.0.1:9092',
  'auto.offset.reset': 'largest',
  'enable.auto.offset.store': false,
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
```

### Kafka AMO(at most once) Consumer

```js
const consumer = new KafkaAMOConsumer({
  'group.id': 'alo-consumer-test-1',
  'metadata.broker.list': '127.0.0.1:9092',
  'auto.offset.reset': 'largest',
  'enable.auto.offset.store': true,
  'enable.auto.commit': true,
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
```
