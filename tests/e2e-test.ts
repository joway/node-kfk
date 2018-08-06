/* tslint:disable */
import test from 'ava'
import * as bluebird from 'bluebird'
import * as _ from 'lodash'
import * as crypto from 'crypto'
import * as sinon from 'sinon'

import { KafkaProducer } from '../src/producer'
import { KafkaALOConsumer, KafkaAMOConsumer } from '../src/consumer'

const BROKERS = '127.0.0.1:9092'
const TOPIC = 'e2e-test'

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
  })
  await producer.connect()
  for (let i = 0; i < mesCount; i++) {
    const msg = `${new Date().getTime()}-${crypto.randomBytes(20).toString('hex')}`
    await producer.produce(topic, null, msg)
  }
  return producer
}

async function setUpConsumer(ConsumerType: any, conf: any, topicConf: any = {}) {
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
  )

  await consumer.connect()
  return consumer
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
  let messages: any[] = []
  while (messages.length < TOTAL) {
    const msgs = await consumer.consume(
      (message: any) => {
        count++
        t.true(count <= TOTAL)
      },
      {
        size: 100,
        concurrency: 100,
      },
    )
    console.log(`consumer fetch ${msgs.length} messages`)
    messages = messages.concat(msgs)
  }
  t.is(messages.length, count)
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

  t.is(count, TOTAL * 2)
})
