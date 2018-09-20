import * as Kafka from 'node-rdkafka'

export const KfkNativeErrorCode = Kafka.CODES.ERRORS

export const KfkErrorCode = {
  UNDEFINED: -1,
  CONNECTION_DEAD: 1,
}

export class KfkError extends Error {
  public code: number = KfkErrorCode.UNDEFINED
}

export class ConnectionDeadError extends KfkError {
  constructor(message: string) {
    super(message)
    this.code = KfkErrorCode.CONNECTION_DEAD
  }
}
