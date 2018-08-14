/* tslint:disable */
import test from 'ava'
import * as bluebird from 'bluebird'
import * as _ from 'lodash'
import * as crypto from 'crypto'
import * as sinon from 'sinon'

import { KafkaProducer } from '../src/producer'
import { KafkaALOConsumer, KafkaAMOConsumer } from '../src/consumer'

const BROKERS = '127.0.0.1:9092'

test.beforeEach(t => {
  t.context.sandbox = sinon.sandbox.create()
})

test.afterEach.always(t => {
  t.context.sandbox.restore()
})

function random(len: number) {
  const possible = 'abcdefghijklmnopqrstuvwxyz'
  return _.sampleSize(possible, len).join('')
}

async function setUpProducer(topic: string, mesCount: number = 100) {
  const producer = new KafkaProducer({
    'client.id': `client-id-test`,
    'compression.codec': 'gzip',
    'metadata.broker.list': BROKERS,
  }, {}, { debug: false })
  await producer.connect()
  for (let i = 0; i < mesCount; i++) {
    const msg = `${new Date().getTime()}-${crypto.randomBytes(20).toString('hex')}`
    await producer.produce(topic, null, msg)
  }
  return producer
}

async function setUpConsumer(ConsumerType: any, conf: any, topicConf: any = {}, options: any = {}) {
  const consumer = new ConsumerType(
    {
      'group.id': 'kafka-group',
      'metadata.broker.list': BROKERS,
      'enable.auto.offset.store': false,
      'enable.auto.commit': false,
      ...conf,
    },
    {
      ...topicConf,
    },
    options,
  )

  await consumer.connect()
  return consumer
}

async function untilFetchMax<T>(handler: Function, maxCount: number) {
  let results: T[] = []
  while (results.length < maxCount) {
    const items = await handler()
    console.log('untilFetchMax fetched ', JSON.stringify(items))
    results = _.concat(results, items)
  }
  return results
}

test('produce', async t => {
  const seed = random(12)
  const topic = `topic-produce-${seed}`
  const group = `group-produce-${seed}`
  console.log('topic', topic, 'group', group)
  const TOTAL = 10
  const producer = await setUpProducer(topic, TOTAL)
  await producer.flush()

  const consumer = await setUpConsumer(
    KafkaALOConsumer,
    {
      'group.id': group,
    },
    {
      'auto.offset.reset': 'earliest',
    },
  )
  consumer.subscribe([topic])

  let count = 0
  const messages: any[] = await untilFetchMax(async () => (
    consumer.consume(
      (message: any) => {
        count++
        t.true(count <= TOTAL)
        return message
      },
      {
        size: 100,
        concurrency: 100,
      },
    )
  ), TOTAL)
  t.is(_.pullAll(messages, undefined).length, count)
  t.is(count, TOTAL)

  await producer.disconnect()
  await consumer.disconnect()
})

test('alo consumer with earliest', async t => {
  const seed = random(12)
  const topic = `topic-alo-${seed}`
  const group = `group-alo-${seed}`
  const TOTAL = 10
  const producer = await setUpProducer(topic, TOTAL)
  await producer.flush()

  // producer msg
  const consumerA = await setUpConsumer(
    KafkaALOConsumer,
    {
      'group.id': group,
    },
    {
      'auto.offset.reset': 'earliest',
    },
  )
  consumerA.subscribe([topic])

  let count = 0
  await consumerA.consume(
    async (message: any) => {
      count++
      t.true(count <= TOTAL)
    },
    {
      size: 100,
      concurrency: 100,
    },
  )
  await consumerA.disconnect()
  t.is(count, TOTAL)

  // producer more msg
  for (let i = 0; i < TOTAL; i++) {
    const msg = `${new Date().getTime()}-${crypto.randomBytes(20).toString('hex')}`
    await producer.produce(topic, null, msg)
  }
  await producer.flush()

  const consumer = await setUpConsumer(
    KafkaALOConsumer,
    {
      'group.id': `${group}-new`,
    },
    {
      'auto.offset.reset': 'earliest',
    },
  )
  consumer.subscribe([topic])
  count = 0
  await consumer.consume(
    async (message: any) => {
      count++
      t.true(count <= TOTAL * 2)
    },
    {
      size: 100,
      concurrency: 100,
    },
  )

  t.is(count, TOTAL * 2)
})

test('alo consumer with latest', async t => {
  const seed = random(12)
  const topic = `topic-produce-${seed}`
  const group = `group-produce-${seed}`
  console.log('topic', topic, 'group', group)
  let beforeCount = 0
  const producer = await setUpProducer(topic, 0)
  for (let i = 0; i < 10; i++) {
    const msg = `${i}`
    beforeCount++
    await producer.produce(topic, null, msg)
  }
  await producer.flush()

  const consumer = await setUpConsumer(
    KafkaALOConsumer,
    {
      'group.id': group,
    },
    {
      'auto.offset.reset': 'latest',
    },
  )
  consumer.subscribe([topic])

  let isHit = false
  await consumer.consume(
    (message: any) => {
      isHit = true
    },
    {
      size: 1,
      concurrency: 1,
    },
  )
  t.false(isHit)

  for (let i = beforeCount + 1; i < beforeCount + 11; i++) {
    const msg = `${i}`
    await producer.produce(topic, null, msg)
  }
  await producer.flush()

  let count = 0
  await consumer.consume(
    (message: any) => {
      const pos = parseInt(message.value.toString('utf-8'))
      count++
      t.true(pos > beforeCount)
    },
    {
      size: 100,
      concurrency: 5,
    },
  )

  t.is(count, 10)
  await producer.disconnect()
  await consumer.disconnect()
})

test('alo consumer with error fallback', async t => {
  const seed = random(12)
  const topic = `topic-produce-${seed}`
  const group = `group-produce-${seed}`
  console.log('topic', topic, 'group', group)
  const TOTAL = 10
  const producer = await setUpProducer(topic, 0)
  for (let i = 0; i < TOTAL; i++) {
    const msg = `${i}`
    await producer.produce(topic, null, msg)
  }
  await producer.flush()

  const consumer = await setUpConsumer(
    KafkaALOConsumer,
    {
      'group.id': group,
    },
    {
      'auto.offset.reset': 'earliest',
    },
    {
      debug: true,
    },
  )
  consumer.subscribe([topic])
  console.log(`subscribed topic ${topic}`)

  let count = 0
  let error_pos: number = -1
  const error_partition = 0
  let error_count = 0
  try {
    await consumer.consume(
      (message: any) => {
        const pos = parseInt(message.value.toString('utf-8'))
        count++
        t.true(count <= TOTAL)
        if (message.partition === error_partition) {
          error_count++
        }
        if (error_pos < 0 && message.partition === error_partition) {
          error_pos = pos
          console.log(`hit error ${error_pos} ${message.partition}`)
          throw Error(`test error ${pos}`)
        }

        console.log(`consumed message ${JSON.stringify(message)}`)
      },
      {
        size: 100,
        concurrency: 10,
      },
    )
  } catch (err) {
    t.true(err.message.includes('test error'))
  }

  console.log(`count ${count}, error_count ${error_count}, error_pos ${error_pos}`)
  const repetition: number[] = []
  await consumer.consume(
    (message: any) => {
      const pos = parseInt(message.value.toString('utf-8'))
      repetition.push(pos)
      return message
    },
    {
      size: 100,
      concurrency: 100,
    },
    {
      debug: true,
    },
  )
  t.true(repetition.includes(error_pos))
  t.is(repetition.length, error_count)

  await producer.disconnect()
  await consumer.disconnect()
})

test('amo consumer with earliest', async t => {
  const seed = random(12)
  const topic = `topic-alo-${seed}`
  const group = `group-alo-${seed}`
  const TOTAL = 10
  const producer = await setUpProducer(topic, TOTAL)
  await producer.flush()

  const consumerA = await setUpConsumer(
    KafkaAMOConsumer,
    {
      'group.id': group,
    },
    {
      'auto.offset.reset': 'earliest',
    },
  )
  consumerA.subscribe([topic])

  let count = 0
  await consumerA.consume(
    async (message: any) => {
      count++
      t.true(count <= TOTAL)
    },
    {
      size: 100,
      concurrency: 100,
    },
  )
  await consumerA.disconnect()
  t.is(count, TOTAL)

  // producer more msg
  for (let i = 0; i < TOTAL; i++) {
    const msg = `${new Date().getTime()}-${crypto.randomBytes(20).toString('hex')}`
    await producer.produce(topic, null, msg)
  }
  await producer.flush()

  const consumer = await setUpConsumer(
    KafkaAMOConsumer,
    {
      'group.id': `${group}-new`,
    },
    {
      'auto.offset.reset': 'earliest',
    },
  )
  consumer.subscribe([topic])
  count = 0
  await consumer.consume(
    async (message: any) => {
      count++
      t.true(count <= TOTAL * 2)
    },
    {
      size: 100,
      concurrency: 100,
    },
  )

  await consumer.disconnect()
  await producer.disconnect()
  t.is(count, TOTAL * 2)
})

test('amo consumer with latest', async t => {
  const seed = random(12)
  const topic = `topic-produce-${seed}`
  const group = `group-produce-${seed}`
  console.log('topic', topic, 'group', group)
  let beforeCount = 0
  const producer = await setUpProducer(topic, 0)
  for (let i = 0; i < 10; i++) {
    const msg = `${i}`
    beforeCount++
    await producer.produce(topic, null, msg)
  }
  await producer.flush()

  const consumer = await setUpConsumer(
    KafkaAMOConsumer,
    {
      'group.id': group,
    },
    {
      'auto.offset.reset': 'latest',
    },
    {
      'debug': true,
    },
  )
  consumer.subscribe([topic])

  let isHit = false
  await consumer.consume(
    (message: any) => {
      isHit = true
    },
    {
      size: 1,
      concurrency: 1,
    },
  )
  t.false(isHit)

  for (let i = beforeCount + 1; i < beforeCount + 11; i++) {
    const msg = `${i}`
    await producer.produce(topic, null, msg)
  }
  await producer.flush()

  let count = 0
  await consumer.consume(
    (message: any) => {
      const pos = parseInt(message.value.toString('utf-8'))
      count++
      t.true(pos > beforeCount)
    },
    {
      size: 100,
      concurrency: 5,
    },
  )

  t.is(count, 10)
  await producer.disconnect()
  await consumer.disconnect()
})

test('amo consumer with error fallback', async t => {
  const seed = random(12)
  const topic = `topic-produce-${seed}`
  const group = `group-produce-${seed}`
  console.log('topic', topic, 'group', group)
  const TOTAL = 10
  const producer = await setUpProducer(topic, 0)
  for (let i = 0; i < TOTAL; i++) {
    const msg = `${i}`
    await producer.produce(topic, null, msg)
  }
  await producer.flush()

  const consumer = await setUpConsumer(
    KafkaAMOConsumer,
    {
      'group.id': group,
    },
    {
      'auto.offset.reset': 'earliest',
    },
    {
      debug: true,
    },
  )
  consumer.subscribe([topic])

  let count = 0
  let error_pos: number = -1
  let error_count = 0
  const error_partition = 0
  try {
    await consumer.consume(
      (message: any) => {
        const pos = parseInt(message.value.toString('utf-8'))
        count++
        t.true(count <= TOTAL)
        if (message.partition === error_partition) {
          error_count++
        }
        if (error_pos < 0 && message.partition === error_partition) {
          error_pos = pos
          throw Error(`test error ${pos}`)
        }
      },
      {
        size: 100,
        concurrency: 100,
      },
    )
  } catch (err) {
    t.true(err.message.includes('test error'))
  }

  const repetition: number[] = []
  await consumer.consume(
    (message: any) => {
      const pos = parseInt(message.value.toString('utf-8'))
      repetition.push(pos)
    },
    {
      size: 100,
      concurrency: 100,
    },
  )
  t.is(repetition.length, 0)

  await producer.disconnect()
  await consumer.disconnect()
})
