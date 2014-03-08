argv = require('optimist')
.usage('Usage: $0')
.argv

#options =
#  config:
#    project_key: argv.projectKey
#    client_id: argv.clientId
#    client_secret: argv.clientSecret

#connector = new Connector options
#connector.run (success) ->
#  process.exit 1 unless success

Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
express = require 'express'
measured = require('measured')
cache = require("lru-cache")

class Meter
  constructor: (name, units) ->
    @meters = _.map units, (unit) ->
      {unit: unit, name: "#{name}Per#{unit.name}", meter: new measured.Meter({rateUnit: unit.rateUnit})}

  mark: () ->
    _.each @meters, (meter) ->
      meter.meter.mark()

  count: () ->
    @meters[0].meter.toJSON().count

  toJSON: () ->
    _.reduce @meters, ((acc, m) -> acc[m.name] = m.meter.toJSON(); acc), {}

class Stats
  constructor: (options) ->
    @units = options.units or [{name: "Second", rateUnit: 1000}, {name: "Minute", rateUnit: 60 * 1000}]
    @started = new Date()
    @panicMode = false
    @paused = false
    @locallyLocked = 0
    @lastTick = -1

    @messagesIn = new Meter "messagesIn", @units
    @messagesOut = new Meter "messagesOut", @units
    @awaitOrderingIn = new Meter "awaitOrderingIn", @units
    @awaitOrderingOut = new Meter "awaitOrderingOut", @units
    @lockedMessages = new Meter "lockedMessages", @units
    @unlockedMessages = new Meter "unlockedMessages", @units
    @newMessages = new Meter "newMessages", @units
    @lockFailedMessages = new Meter "lockFailedMessages", @units
    @processedSuccessfully = new Meter "processedSuccessfully", @units
    @processingErrors = new Meter "processingErrors", @units

  toJSON: (countOnly = false)->
    started: @started
    lastTick: @lastTick
    messagesIn: if countOnly then @messagesIn.count() else @messagesIn.toJSON()
    messagesOut: if countOnly then @messagesOut.count() else @messagesOut.toJSON()
    awaitOrderingIn: if countOnly then @awaitOrderingIn.count() else @awaitOrderingIn.toJSON()
    awaitOrderingOut: if countOnly then @awaitOrderingOut.count() else @awaitOrderingOut.toJSON()
    messagesInProgress: @messagesInProgress()
    locallyLocked: @locallyLocked
    lockedMessages: if countOnly then @lockedMessages.count() else @lockedMessages.toJSON()
    unlockedMessages: if countOnly then @unlockedMessages.count() else @unlockedMessages.toJSON()
    newMessages: if countOnly then @newMessages.count() else @newMessages.toJSON()
    lockFailedMessages: if countOnly then @lockFailedMessages.count() else @lockFailedMessages.toJSON()
    processingErrors: if countOnly then @processingErrors.count() else @processingErrors.toJSON()
    processedSuccessfully: if countOnly then @processedSuccessfully.count() else @processedSuccessfully.toJSON()
    panicMode: @panicMode
    paused: @paused

  applyBackpressureAtTick: (tick) ->
    @lastTick = tick
    @paused or @panicMode or @messagesInProgress() > 0

  messagesInProgress: () ->
    @messagesIn.count() - @messagesOut.count()

  incommingMessage: (msg) ->
    @messagesIn.mark()
    msg

  newMessage: (msg) ->
    @newMessages.mark()

  lockedMessage: (msg) ->
    @lockedMessages.mark()

  unlockedMessage: (msg) ->
    @unlockedMessages.mark()

  messageFinished: (msg) ->
    @messagesOut.mark()

  failedLock: (msg) ->
    @lockFailedMessages.mark()

  processingError: (msg) ->
    @processingErrors.mark()

  yay: (msg) ->
    @processedSuccessfully.mark()

  awaitOrderingAdded: (msg) ->
    @awaitOrderingIn.mark()

  awaitOrderingRemoved: (msg) ->
    @awaitOrderingOut.mark()


  _initiateSelfDestructionSequence: ->
    Rx.Observable.interval(500).subscribe =>
      if @messagesInProgress() is 0
        console.info "Graceful exit", @toJSON()
        process.exit 0

  startServer: (port) ->
    statsApp = express()

    statsApp.get '/', (req, res) =>
      res.json @toJSON()

    statsApp.get '/count', (req, res) =>
      res.json @toJSON(true)

    statsApp.get '/stop', (req, res) =>
      res.json
        message: 'Self destruction sequence initiated!'

      @panicMode = true
      @_initiateSelfDestructionSequence()

    statsApp.get '/pause', (req, res) =>
      if @paused
        res.json
          message: "Already paused."
      else
        @paused = true
        res.json
          message: "Done."

    statsApp.get '/resume', (req, res) =>
      if not @paused
        res.json
          message: "Already running."
      else
        @paused = false
        res.json
          message: "Done."

    statsApp.listen port, ->
      console.log "Statistics is on port 7777"

  startPrinter: (countOnly = false) ->
    Rx.Observable.interval(3000).subscribe =>
      console.info "+-------------------------------"
      console.info @toJSON(countOnly)
      console.info "+-------------------------------"

class BatchMessageService
  constructor: ->

  getMessages: ->
    # TODO
    return Rx.Observable.fromArray [
      {id: 3, resource: {typeId: "order", id: 1}, sequenceNumber: 3, name: "b"},
      {id: 1, resource: {typeId: "order", id: 1}, sequenceNumber: 1, name: "a"},
      {id: 2, resource: {typeId: "order", id: 1}, sequenceNumber: 2, name: "c"}]

class SphereService
  constructor: (@stats, options) ->

  getLastProcessedSequenceNumber: (resource) ->
    # TODO
    Q(0)

  reportSuccessfullProcessing: (msg) ->
    console.info "Success: ", msg
    # TODO
    Q(msg)

  reportMessageProcessingFailure: (msg, error, processor) ->
    console.error msg, error.stack
    # TODO
    Q(msg)

class MessagePersistenceService
  constructor: (@stats, @sphere, options) ->
    @checkInterval = options.checkInterval or 2000
    @awaitTimeout = options.awaitTimeout or 10000
    @sequenceNumberCacheOptions = options.sequenceNumberCacheOptions or {max: 1000, maxAge: 20 * 1000}

    @processedMessages = []
    @localLocks = []
    @awaitingMessages = []
    @sequenceNumberCache = cache(@sequenceNumberCacheOptions)

    @_startAwaitingMessagesChecker()

  _startAwaitingMessagesChecker: () ->
    Rx.Observable.interval @checkInterval
    .subscribe =>
      [outdated, stillAwaiting] = _.partition @awaitingMessages, (a) =>
        Date.now() - a.added > @awaitTimeout

      @awaitingMessages = stillAwaiting

      _.each outdated, (out) =>
        @_getLastProcessedSequenceNumber out.message
        .then (lastProcessedSN) =>
          @stats.awaitOrderingRemoved out.message

          if out.message.sequenceNumber is (lastProcessedSN + 1)
            out.sink.onNext out.message
            out.sink.onCompleted()
            out.errors.onCompleted()
          else if out.message.sequenceNumber <= lastProcessedSN
            @_messageHasLowSequenceNumber out.message
            out.recycleBin.onNext out.message
            out.sink.onCompleted()
            out.errors.onCompleted()
          else
            out.recycleBin.onNext out.message
            out.errors.onCompleted()
            out.sink.onComplete()
        .fail (error) =>
          @stats.awaitOrderingRemoved out.message

          out.errors.onNext {message: out.message, error: error, processor: "Get last processed sequence number"}
          out.errors.onCompleted()
          out.sink.onCompleted()
        .done()

  _checkAwaiting: (justProcessedMsg) ->
    [toSink, stillAwaiting] = _.partition @awaitingMessages, (a) =>
      @_snCacheKey(a.message.resource) is @_snCacheKey(justProcessedMsg.resource) and a.message.sequenceNumber is (justProcessedMsg.sequenceNumber + 1)

    @awaitingMessages = stillAwaiting

    _.each toSink, (s) =>
      @stats.awaitOrderingRemoved s.message

      s.sink.onNext s.message
      s.sink.onCompleted()
      s.errors.onCompleted()

  checkAvaialbleForProcessingAndLockLocally: (msg) ->
    @isProcessed msg
    .then (processed) =>
      available = not processed and not @hasLocalLock(msg)

      if available
        @takeLocalLock msg

      available

  hasLocalLock: (msg) ->
    _.contains(@localLocks, msg.id)

  takeLocalLock: (m) ->
    @stats.locallyLocked = @stats.locallyLocked + 1
    @localLocks.push m.id

  releaseLocalLock: (m) ->
    if @hasLocalLock(m)
      @stats.locallyLocked = @stats.locallyLocked - 1

    @localLocks = _.without @localLocks, m.id

  isProcessed: (msg) ->
    # TODO
    Q(_.contains(@processedMessages, msg.id))

  lockMessage: (msg) ->
    # TODO
    Q({message: msg, lock: {}})

  unlockMessage: (msg) ->
    # TODO
    Q(msg)

  reportMessageProcessingFailure: (msg, error, processor) ->
    @sphere.reportMessageProcessingFailure msg, error, processor

  reportSuccessfullProcessing: (msg) ->
    @sphere.reportSuccessfullProcessing msg
    .then (msg) =>
      # We've done it!! We processed message successfully! (let's hope we will also unlock it successfully too)
      @stats.yay msg

      alreadyInCache = @sequenceNumberCache.get @_snCacheKey(msg.resource)

      if not alreadyInCache? or alreadyInCache < msg.sequenceNumber
        @sequenceNumberCache.set @_snCacheKey(msg.resource), msg.sequenceNumber
        @_checkAwaiting msg

      msg

  _messageHasLowSequenceNumber: (msg) ->
    console.info "WARN: message has appeared twice for processing (something wrong in the pipeline)", msg

  orderBySequenceNumber: (msg, lock, recycleBin) ->
    sink = new Rx.Subject()
    errors = new Rx.Subject()

    @_getLastProcessedSequenceNumber msg
    .then (lastProcessedSN) =>
      if msg.sequenceNumber is (lastProcessedSN + 1)
        sink.onNext msg
        sink.onCompleted()
        errors.onCompleted()
      else if msg.sequenceNumber <= lastProcessedSN
        @_messageHasLowSequenceNumber msg
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
          lock: lock
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
    cached = @sequenceNumberCache.get @_snCacheKey(msg.resource)

    if cached?
      Q(cached)
    else
      @sphere.getLastProcessedSequenceNumber msg.resource
      .then (sn) =>
        alreadyInCache = @sequenceNumberCache.get @_snCacheKey(msg.resource)

        if not alreadyInCache? or alreadyInCache < sn
          @sequenceNumberCache.set @_snCacheKey(msg.resource), sn

        sn

class MessageProcessor
  constructor: (@stats, @messageService, @persistenceService, options) ->
    @messageProcessors = options.processors # Array[Message => Promise[Something]]
    @tickDelay = options.tickDelay or 500

    @recycleBin = @_createRecycleBin()
    @errors = @_createErrorProcessor(@recycleBin)
    @unrecoverableErrors = @_createUnrecoverableErrorProcessor(@recycleBin)

  run: () ->
    all = @_allMessages()
    maybeLocked = @_filterAndLockNewMessages(all)
    processed = @_doProcessMessages(maybeLocked)

    processed.subscribe @recycleBin

  _doProcessMessages: (messages) ->
    [locked, nonLocked, errors]  = @_split messages, (box) ->
      Q(box.lock?)

    # nothing can really go wrong here
    errors.subscribe @unrecoverableErrors

    nonLocked
    .map (box) ->
      box.message
    .do (msg) =>
      @stats.failedLock msg
    .subscribe @recycleBin

    locked
    .do (box) =>
      @stats.lockedMessage box.message
    .flatMap (box) =>
      [sink, errors] = @persistenceService.orderBySequenceNumber box.message, box.lock, @recycleBin

      errors.subscribe @errors
      sink
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @_processMessage(@messageProcessors, msg)
      .then (res) ->
        msg.result = res
        subj.onNext(msg)
        subj.onCompleted()
      .fail (error) =>
        @errors.onNext {message: msg, error: error, processor: "Actual processing"}
        subj.onCompleted()
      .done()

      subj
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @persistenceService.reportSuccessfullProcessing msg
      .then (msg) ->
        subj.onNext(msg)
        subj.onCompleted()
      .fail (error) ->
        @errors.onNext {message: msg, error: error, processor: "Reporting successful processing"}
        subj.onComplete()
      .done()

      subj
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @persistenceService.unlockMessage msg
      .then (msg) ->
        subj.onNext(msg)
        subj.onCompleted()
      .fail (error) ->
        @errors.onNext {message: msg, error: error, processor: "Unlocking the message"}
        subj.onComplete()
      .done()

      subj
    .do (msg) ->
      stats.unlockedMessage msg

  _processMessage: (processors, msg) ->
    try
      promises = _.map processors, (processor) ->
        processor msg.message

      Q.all promises
    catch error
      Q.reject(error)

  _filterAndLockNewMessages: (messages) ->
    [newMessages, other, errors]  = @_split messages, (msg) =>
      @persistenceService.checkAvaialbleForProcessingAndLockLocally msg

    other.subscribe @recycleBin
    errors.subscribe @unrecoverableErrors

    newMessages
    .do (msg) =>
      @stats.newMessage(msg)
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @persistenceService.lockMessage msg
      .then (maybeLocked) ->
        subj.onNext(maybeLocked)
        subj.onCompleted()
      .fail (error) =>
        @unrecoverableErrors.onNext {message: msg, error: error, "Locking the message"}
        subj.onCompleted()
      .done()

      subj

  _allMessages: () =>
    Rx.Observable.interval(@tickDelay)
    .filter (tick) =>
      not @stats.applyBackpressureAtTick(tick)
    .flatMap =>
      @messageService.getMessages()
    .map (msg) =>
      @stats.incommingMessage msg

  _createRecycleBin: () ->
    recycleBin = new Rx.Subject()

    recycleBin
    .subscribe (msg) =>
      @persistenceService.releaseLocalLock msg
      @stats.messageFinished msg

    recycleBin

  _createErrorProcessor: (unrecoverableErrors, recycleBin) ->
    errorProcessor = new Rx.Subject()

    errorProcessor
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @stats.processingError msg
      @persistenceService.reportMessageProcessingFailure msg.message, msg.error, msg.processor
      .then (msg) ->
        subj.onNext msg
        subj.onCompleted()
      .fail (error) ->
        unrecoverableErrors.onNext {message: msg, error: error, processor: "Reporting processing error"}
        subj.onCompleted()
      .done()

      subj
    .subscribe recycleBin

    errorProcessor

  _createUnrecoverableErrorProcessor: (resycleBin) ->
    errorProcessor = new Rx.Subject()

    errorProcessor
    .flatMap (box) ->
      # TODO
      console.error "Error during: #{box.processor}"
      console.error box.error.stack

    errorProcessor

  _split: (obs, predicatePromice) ->
    thenSubj = new Rx.Subject()
    elseSubj = new Rx.Subject()
    errSubj = new Rx.Subject()

    nextFn = (x) ->
      predicatePromice x
      .then (bool) ->
        if bool
          thenSubj.onNext x
        else
          elseSubj.onNext x
      .fail (error) ->
        errSubj.onNext {message: x, error: error, processor: "Split predicate"}
      .done()

    obs.subscribe(
      nextFn,
      ((error) -> errSubj.onNext {message: null, error: error, processor: "Split"}),
      ( -> thenSubj.onCompleted(); elseSubj.onCompleted(); errSubj.onCompleted()),
    )

    [thenSubj, elseSubj,errSubj]

stats = new Stats {}
sphereService = new SphereService stats, {}
messageService = new BatchMessageService()
persistenceService = new MessagePersistenceService stats, sphereService, {}
messageProcessor = new MessageProcessor stats, messageService, persistenceService,
  processors: [
    (msg) -> Q("Done1")
    (msg) -> Q("Done2")
  ]

stats.startServer(7777)
stats.startPrinter(true)

messageProcessor.run()