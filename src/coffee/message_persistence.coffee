Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
cache = require 'lru-cache'
measured = require 'measured'

class MessagePersistenceService
  constructor: (@stats, @sphere, options) ->
    @checkInterval = options.checkInterval or 2000
    @awaitTimeout = options.awaitTimeout or 120000
    @sequenceNumberCacheOptions = options.sequenceNumberCacheOptions or {max: 20000, maxAge: 20 * 1000}
    @processedMessagesCacheOptions = options.processedMessagesCacheOptions or {max: 30000, maxAge: 24 * 60 * 60 * 1000}

    @processedMessages = []
    @localLocks = []
    @awaitingMessages = []
    @sequenceNumberCache = cache @sequenceNumberCacheOptions
    @processedMessagesCache = cache @processedMessagesCacheOptions
    @panicMode = false

    @stats.addCustomStat @getSourceInfo().prefix, "sequenceNumberCacheSize", =>
      _.size @sequenceNumberCache
    @stats.addCustomStat @getSourceInfo().prefix, "processedMessagesCacheSize", =>
      _.size @processedMessagesCache
    @stats.cacheClearCommands.subscribe =>
      @sequenceNumberCache.reset()
      @processedMessagesCache.reset()
    @stats.panicModeEvents.subscribe =>
      @panicMode = true


    @_startAwaitingMessagesChecker()

  _startAwaitingMessagesChecker: () ->
    Rx.Observable.interval @checkInterval
    .subscribe =>
      [outdated, stillAwaiting] = _.partition @awaitingMessages, (a) =>
        @panicMode or (Date.now() - a.added > @awaitTimeout)

      @awaitingMessages = stillAwaiting

      _.each outdated, (out) =>
        @_getLastProcessedSequenceNumber out.message
        .then (lastProcessedSN) =>
          @stats.awaitOrderingRemoved out.message

          if out.message.payload.sequenceNumber is (lastProcessedSN.sequenceNumber + 1)
            @_doSinkMessage lastProcessedSN, out
          else if out.message.payload.sequenceNumber <= lastProcessedSN.sequenceNumber
            @_messageHasLowSequenceNumber out.message
            out.recycleBin.onNext out.message
            out.sink.onCompleted()
            out.errors.onCompleted()
          else
            out.recycleBin.onNext out.message
            out.sink.onCompleted()
            out.errors.onCompleted()
        .fail (error) =>
          @stats.awaitOrderingRemoved out.message

          out.errors.onNext {message: out.message, error: error, processor: "Get last processed sequence number"}
          out.errors.onCompleted()
          out.sink.onCompleted()
        .done()

  _checkAwaiting: (justProcessedMsg) ->
    [toSink, stillAwaiting] = _.partition @awaitingMessages, (a) =>
      @_snCacheKey(a.message.payload.resource) is @_snCacheKey(justProcessedMsg.payload.resource) and a.message.payload.sequenceNumber is (justProcessedMsg.payload.sequenceNumber + 1)

    @awaitingMessages = stillAwaiting

    _.each toSink, (s) =>
      @stats.awaitOrderingRemoved s.message
      @_doSinkMessage {sequenceNumber: justProcessedMsg.payload.sequenceNumber, cached: true}, s

  getSourceInfo: () ->
    @sphere.getSourceInfo()

  getMessageSource: () ->
    [observer, observable] = @sphere.getMessageSource()

    newObservable = observable
    .map (msg) =>
      {payload: msg, persistence: this}

    [observer, newObservable]

  checkAvaialbleForProcessingAndLockLocally: (msg) ->
    @isProcessed msg
    .then (processed) =>
      available = not processed and not @hasLocalLock(msg)

      if available
        @takeLocalLock msg

      available

  hasLocalLock: (msg) ->
    _.contains(@localLocks, msg.payload.id)

  takeLocalLock: (msg) ->
    @stats.locallyLocked = @stats.locallyLocked + 1
    @localLocks.push msg.payload.id

  releaseLocalLock: (msg) ->
    if @hasLocalLock(msg)
      @stats.locallyLocked = @stats.locallyLocked - 1

    @localLocks = _.without @localLocks, msg.payload.id

  isProcessed: (msg) ->
    Q(@processedMessagesCache.get(msg.payload.id)?)

  lockMessage: (msg) ->
    sink = new Rx.Subject()
    errors = new Rx.Subject()
    skip = new Rx.Subject()

    @sphere.lockMessage msg.payload
    .then (lock) =>
      if lock.type is 'existing'
        skip.onNext msg

        if lock.payload.value.state is 'processed'
          @processedMessagesCache.set msg.payload.id, "Processed!"
        else
          @stats.failedLock msg
      else if lock.type is 'new'
        @stats.lockedMessage msg
        msg.lock = lock.payload
        sink.onNext msg
      else
        errors.onNext {message: msg, error: new Error("Unsupported lock type: #{lock.type}"), processor: "Locking the message"}

      errors.onCompleted()
      sink.onCompleted()
      skip.onCompleted()
    .fail (error) ->
      errors.onNext {message: msg, error: error, processor: "Locking the message"}
      errors.onCompleted()
      sink.onCompleted()
      skip.onCompleted()
    .done()

    [sink, errors, skip]

  unlockMessage: (msg) ->
    @sphere.unlockMessage msg.payload, msg.lock
    .then =>
      @stats.unlockedMessage msg
      msg

  reportMessageProcessingFailure: (msg, error, processor) ->
    @sphere.reportMessageProcessingFailure msg.payload, msg.lock, error, processor
    .then ->
      msg

  reportSuccessfullProcessing: (msg) ->
    @sphere.reportSuccessfullProcessing msg.payload, msg.lock, msg.result
    .then =>
      # We've done it!! We processed message successfully! (let's hope we will also unlock it successfully too)
      @stats.yay msg

      @processedMessagesCache.set msg.payload.id, "Processed!"

      alreadyInCache = @sequenceNumberCache.get @_snCacheKey(msg.payload.resource)

      if not alreadyInCache? or alreadyInCache < msg.payload.sequenceNumber
        @sequenceNumberCache.set @_snCacheKey(msg.payload.resource), msg.payload.sequenceNumber
        @_checkAwaiting msg

      msg

  _messageHasLowSequenceNumber: (msg) ->
    console.info "WARN: message has appeared twice for processing (something wrong in the pipeline)", msg.payload

  _doSinkMessage: (sn, box) ->
    doSink = () ->
      box.sink.onNext box.message
      box.sink.onCompleted()
      box.errors.onCompleted()

    if sn.cached
      @sphere.getLastProcessedSequenceNumber box.message.payload.resource
      .then (upToDateSN) ->
        if upToDateSN is sn.sequenceNumber
          doSink()
        else
          # looks like already processed somewhere else
          box.recycleBin.onNext box.message
          box.errors.onCompleted()
          box.sink.onCompleted()
      .fail (error) ->
        box.errors.onNext {message: box.message, error: error, processor: "Get last processed sequence number during sinking"}
        box.errors.onCompleted()
        box.sink.onCompleted()
    else
      doSink()

  orderBySequenceNumber: (msg, recycleBin) ->
    sink = new Rx.Subject()
    errors = new Rx.Subject()

    @_getLastProcessedSequenceNumber msg
    .then (lastProcessedSN) =>
      if msg.payload.sequenceNumber is (lastProcessedSN.sequenceNumber + 1)
        @_doSinkMessage lastProcessedSN,
          message: msg
          sink: sink
          errors: errors
          recycleBin: recycleBin
      else if msg.payload.sequenceNumber <= lastProcessedSN.sequenceNumber
        @_messageHasLowSequenceNumber msg
        recycleBin.onNext msg
        sink.onCompleted()
        errors.onCompleted()
      else if @panicMode
        recycleBin.onNext msg
        sink.onCompleted()
        errors.onCompleted()
      else
        @stats.awaitOrderingAdded msg
        @awaitingMessages.push
          message: msg
          sink: sink
          errors: errors
          recycleBin: recycleBin
          added: Date.now()
    .fail (error) ->
      errors.onNext {message: msg, error: error, processor: "Get last processed sequence number"}
      errors.onCompleted()
      sink.onCompleted()
    .done()

    [sink, errors]

  _snCacheKey: (res) ->
    "#{res.typeId}-#{res.id}"

  _getLastProcessedSequenceNumber: (msg) ->
    cached = @sequenceNumberCache.get @_snCacheKey(msg.payload.resource)

    if cached?
      Q({sequenceNumber: cached, cached: true})
    else
      @sphere.getLastProcessedSequenceNumber msg.payload.resource
      .then (sn) =>
        alreadyInCache = @sequenceNumberCache.get @_snCacheKey(msg.payload.resource)

        if not alreadyInCache? or alreadyInCache < sn
          @sequenceNumberCache.set @_snCacheKey(msg.payload.resource), sn

        {sequenceNumber: sn, cached: false}

exports.MessagePersistenceService = MessagePersistenceService