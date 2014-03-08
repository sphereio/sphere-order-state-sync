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
    @lockedMessages = new Meter "lockedMessages", @units
    @unlockedMessages = new Meter "unlockedMessages", @units
    @newMessages = new Meter "newMessages", @units
    @lockFailedMessages = new Meter "lockFailedMessages", @units
    @processingErrors = new Meter "processingErrors", @units

  toJSON: ->
    started: @started
    lastTick: @lastTick
    messagesIn: @messagesIn.toJSON()
    messagesOut: @messagesOut.toJSON()
    locallyLocked: @locallyLocked
    lockedMessages: @lockedMessages.toJSON()
    unlockedMessages: @unlockedMessages.toJSON()
    newMessages: @newMessages.toJSON()
    lockFailedMessages: @lockFailedMessages.toJSON()
    processingErrors: @processingErrors.toJSON()
    panicMode: @panicMode
    paused: @paused

  applyBackpressureAtTick: (tick) ->
    @lastTick = tick
    @paused or @panicMode or @messagesInProgress() is not 0

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

  _initiateSelfDestructionSequence: ->
    Rx.Observable.interval(500).subscribe =>
      if @messagesInProgress() is 0
        console.info "Graceful exit", @toJSON()
        process.exit 0

  startServer: (port) ->
    statsApp = express()

    statsApp.get '/', (req, res) =>
      res.json @toJSON()

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

  startPrinter: () ->
    Rx.Observable.interval(3000).subscribe =>
      console.info "+-------------------------------"
      console.info @toJSON()
      console.info "+-------------------------------"

class BatchMessageService
  constructor: ->

  getMessages: ->
    # TODO
    return Rx.Observable.fromArray([{id: 1, name: "a"}, {id: 2, name: "b"}, {id: 3, name: "c"}])

class SphereService
  constructor: (@stats, options) ->

class MessagePersistenceService
  constructor: (@stats, @sphere) ->
    @processedMessages = []
    @localLocks = []

  checkAvaialbleForProcessingAndLockLocally: (msg) ->
    @isProcessed msg
    .then (processed) =>
      available = not processed and not @hasLocalLock(msg)

      if available
        @takeLocalLock msg

      available

  hasLocalLock: (msg) ->
    _.contains(@localLocks, msg.id)

  isProcessed: (msg) ->
    Q(_.contains(@processedMessages, msg.id))

  takeLocalLock: (m) ->
    @stats.locallyLocked = @stats.locallyLocked + 1
    @localLocks.push m.id

  releaseLocalLock: (m) ->
    if @hasLocalLock(m)
      @stats.locallyLocked = @stats.locallyLocked - 1

    @localLocks = _.without @localLocks, m.id

  lockMessage: (msg) ->
    # TODO
    Q({message: msg, lock: {}})

  unlockMessage: (msg) ->
    # TODO
    Q(msg)

  orderBySequenceNumber: (msg, lock, recycleBin) ->
    # TODO
    Rx.Observable.return(msg)

  reportMessageProcessingFailure: (msg, error) ->
    console.error msg, error.stack
    # TODO
    Q(msg)

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
      @persistenceService.orderBySequenceNumber box.message, box.lock, @recycleBin
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @_processMessage(@messageProcessors, msg)
      .then (res) ->
        msg.result = res
        subj.onNext(msg)
        subj.onCompleted()
      .fail (error) =>
        @errors.onNext {message: msg, error: error}
        subj.onCompleted()
      .done()

      subj
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @persistenceService.unlockMessage msg
      .then (msg) ->
        subj.onNext(msg)
        subj.onCompleted()
      .fail (error) ->
        @unrecoverableErrors.onNext {message: msg, error: error}
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
        @unrecoverableErrors.onNext {message: msg, error: error}
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
      @persistenceService.reportMessageProcessingFailure msg.message, msg.error
      .then (msg) ->
        subj.onNext msg
        subj.onCompleted()
      .fail (error) ->
        unrecoverableErrors.onNext {message: msg, error: error}
        subj.onCompleted()
      .done()

      subj
    .subscribe recycleBin

    errorProcessor

  _createUnrecoverableErrorProcessor: (resycleBin) ->
    errorProcessor = new Rx.Subject()

    errorProcessor
    .flatMap (box) =>
      # TODO
      console.error box.error

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
        errSubj.onNext {message: x, error: error}
      .done()

    obs.subscribe(
      nextFn,
      ((error) -> errSubj.onNext {message: null, error: error}),
      ( -> thenSubj.onCompleted(); elseSubj.onCompleted(); errSubj.onCompleted()),
    )

    [thenSubj, elseSubj,errSubj]

stats = new Stats {}
sphereService = new SphereService stats
messageService = new BatchMessageService()
persistenceService = new MessagePersistenceService stats, sphereService
messageProcessor = new MessageProcessor stats, messageService, persistenceService,
  processors: [
    (msg) -> Q("Done1")
    (msg) -> Q("Done2")
  ]

stats.startServer(7777)
stats.startPrinter()

messageProcessor.run()