import test from 'ava'

import * as sinon from 'sinon'
import * as request from 'supertest'

test.beforeEach(t => {
  t.context.sandbox = sinon.sandbox.create()
})

test.afterEach.always(t => {
  t.context.sandbox.restore()
})
