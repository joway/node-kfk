export const KfkErrorCode = {
  UNDEFINED: -1,

  CONNECTING: 1,
  CONNECTED: 2,
  DISCONNECT: 3,
  CONNECTION_NOT_READY: 4,
  CONNECTION_DEAD: 5,

  PRODUCER_RUNTIME: 101,
  PRODUCER_FLUSH: 102,

  METADATA: 201,
  SEEK: 202,
  CONSUMER_RUNTIME: 203,
}

export class KfkError extends Error {
  public code: number = KfkErrorCode.UNDEFINED
}

// --- connection ---
export class ConnectingError extends KfkError {
  constructor(message: string) {
    super(message)
    this.code = KfkErrorCode.CONNECTING
  }
}
export class ConnectedError extends KfkError {
  constructor(message: string) {
    super(message)
    this.code = KfkErrorCode.CONNECTED
  }
}
export class DisconnectError extends KfkError {
  constructor(message: string) {
    super(message)
    this.code = KfkErrorCode.DISCONNECT
  }
}
export class ConnectionNotReadyError extends KfkError {
  constructor(message: string) {
    super(message)
    this.code = KfkErrorCode.CONNECTION_NOT_READY
  }
}
export class ConnectionDeadError extends KfkError {
  constructor(message: string) {
    super(message)
    this.code = KfkErrorCode.CONNECTION_DEAD
  }
}

// --- producer ---
export class ProducerRuntimeError extends KfkError {
  constructor(message: string) {
    super(message)
    this.code = KfkErrorCode.PRODUCER_RUNTIME
  }
}
export class ProducerFlushError extends KfkError {
  constructor(message: string) {
    super(message)
    this.code = KfkErrorCode.PRODUCER_FLUSH
  }
}

// consumer
export class MetadataError extends KfkError {
  constructor(message: string) {
    super(message)
    this.code = KfkErrorCode.METADATA
  }
}
export class SeekError extends KfkError {
  constructor(message: string) {
    super(message)
    this.code = KfkErrorCode.SEEK
  }
}
export class ConsumerRuntimeError extends KfkError {
  constructor(message: string) {
    super(message)
    this.code = KfkErrorCode.CONSUMER_RUNTIME
  }
}
