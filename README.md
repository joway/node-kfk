# Node-Kfk

[![Build Status](https://travis-ci.org/joway/node-kfk.svg?branch=master)](https://travis-ci.org/joway/node-kfk)
[![npm](https://img.shields.io/npm/v/kfk.svg)](https://www.npmjs.com/package/kfk)
[![npm](https://img.shields.io/npm/dt/kfk.svg)](https://www.npmjs.com/package/kfk)

The high-level node kafka client based on node-rdkafka .

`KafkaALOConsumer` will monitor your consume callback function execute state and fallback to the offset when any error occur . If there are any `Error` throwed out in your consumer callback function , it will to been **blocked** on the offset where the error throw . It guarantee that all messages will been consumed at least once .

`KafkaAMOConsumer` is a simple consumer and auto commits the offsets when fetched the messsages . It has better performance than `KafkaALOConsumer`, but not guarantee that all messages will been consumed. In `KafkaALOConsumer `, one message will be consumed at most once .

Even using `KafkaAMOConsumer` , you can also take some Fault-Tolerance technology such as retry policy to ensure that message will be consumed correctly as far as possible .

If your don't care little messages lose (when disaster occur), we recommend you to use `KafkaAMOConsumer` for better performance .

The client Has been tested on:

```yaml
- os: linux
  env: KAFKA_VERSION=0.10.2.2
  node_js: 8
- os: linux
  env: KAFKA_VERSION=0.10.2.2
  node_js: 10
- os: linux
  env: KAFKA_VERSION=0.11.0.3
  node_js: 10
- os: linux
  env: KAFKA_VERSION=1.1.0
  node_js: 10
- os: linux
  env: KAFKA_VERSION=2.0.0
  node_js: 10
```

More detailed document for `conf` and `topicConf` in [librdkafka](https://github.com/edenhill/librdkafka) and [node-rdkafka](https://github.com/Blizzard/node-rdkafka)

## Usage

### Install

```shell
npm i kfk -S
```

### Kafka Producer

```js
const conf = {
  'client.id': 'kafka',
  'metadata.broker.list': '127.0.0.1:9092',
  'compression.codec': 'gzip',
  'socket.keepalive.enable': true,
}
const topicConf = {
}
const options = {
  debug: false,
}

const producer = new KafkaProducer(conf, topicConf, options)

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
const conf = {
  'group.id': 'alo-consumer-test-1',
  'metadata.broker.list': '127.0.0.1:9092',
}
const topicConf = {
  'auto.offset.reset': 'largest',
}
const options = {
  debug: false,
}

const consumer = new KafkaALOConsumer(conf, topicConf, options)
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
const conf = {
  'group.id': 'amo-consumer-test-1',
  'metadata.broker.list': '127.0.0.1:9092',
}
const topicConf = {
  'auto.offset.reset': 'largest',
}
const options = {
  debug: false,
}

const consumer = new KafkaAMOConsumer(conf, topicConf, options)
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
