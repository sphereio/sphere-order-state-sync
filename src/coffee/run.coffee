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

messageProcessors = [((m)-> Q(m)), ((m)-> Q(m)), ((m)-> Q(m))]

processMessage = (processors, msg) ->
  promises = _.map processors, (p) ->
    p(msg)

  subj = new Rx.Subject()

  Q.all(promises)
  .then (res) ->
    msg.processingResults = res
    subj.onNext(msg)
    subj.onCompleted()
  .fail (error) ->
    msg.processingResults = error
    subj.onNext(msg)
    subj.onCompleted()
  .done()

  subj

class Stats
  constructor: ->
    @started = new Date()
    @messagesIn = 0
    @messagesOut = 0
    @locallyLocked = 0
    @lockedMessages = 0
    @newMessages = 0
    @lockFailedMessages = 0
    @panicMode = false

  get: ->
    started: @started
    messagesIn: @messagesIn
    messagesOut: @messagesOut
    locallyLocked: @locallyLocked
    lockedMessages: @lockedMessages
    newMessages: @newMessages
    lockFailedMessages: @lockFailedMessages
    panicMode: @panicMode

  applyBackpressureAtTick: (tick) ->
    @lastTick = tick
    @panicMode or @messagesInProgress() is not 0

  messagesInProgress: () ->
    @messagesIn - @messagesOut

  incommingMessage: (msg) ->
    @messagesIn = @messagesIn + 1
    msg

  newMessage: (msg) ->
    @newMessages = @newMessages + 1

  lockedMessage: (msg) ->
    @lockedMessages = @lockedMessages + 1

  unlockedMessage: (msg) ->
    @lockedMessages = @lockedMessages + 1

  messageFinished: (msg) ->
    @messagesOut = @messagesOut + 1

  failedLock: (msg) ->
    @lockFailedMessages = @lockFailedMessages + 1

class BatchMessageService
  constructor: ->

  getMessages: ->
    # TODO
    return Rx.Observable.fromArray([{id: 1, name: "a"}, {id: 2, name: "b"}, {id: 3, name: "c"}])


class MessagePersistenceService
  constructor: (@stats) ->
    @processedMessages = [2]
    @localLocks = []

  checkAvaialbleForProcessingAndLockLocally: (msg) ->
    available = not @isProcessed(msg) and not @hasLocalLock(msg)

    if available
      @takeLocalLock msg

    available

  hasLocalLock: (msg) ->
    _.contains(@localLocks, msg.id)

  isProcessed: (msg) ->
    _.contains(@processedMessages, msg.id)

  takeLocalLock: (m) ->
    @stats.locallyLocked = @stats.locallyLocked + 1
    @localLocks.push m.id

  releaseLocalLock: (m) ->
    if @hasLocalLock(m)
      @stats.locallyLocked = @stats.locallyLocked - 1

    @localLocks = _.without @localLocks, m.id

  lockMessage: (msg) ->
    # TODO
    Rx.Observable.return(msg).map (m) ->
      m.locked = true
      m

  unlockMessage: (msg) ->
    # TODO
    Rx.Observable.return(msg).map (m) ->
      m.locked = false
      m

  orderBySequenceNumber: (msg, recycleBin) ->
    # TODO
    Rx.Observable.return(msg)

  reportMessageProcessingFailure: (msg, error) ->
    # TODO
    Rx.Observable.return(msg)


stats = new Stats()
messageService = new BatchMessageService()
persistenceService = new MessagePersistenceService(stats)

allMessages = Rx.Observable
.interval(500)
.filter (tick) ->
  not stats.applyBackpressureAtTick(tick)
.flatMap ->
  messageService.getMessages()
.map (msg) ->
  stats.incommingMessage msg


ifSplit = (obs, predicate) ->
  thenSubj = new Rx.Subject()
  elseSubj = new Rx.Subject()

  obs.subscribe(
    ((x) -> if (predicate(x)) then thenSubj.onNext(x) else elseSubj.onNext(x)),
    ((error) -> console.error error.stack),
    ( -> thenSubj.onCompleted(); elseSubj.onCompleted()),
  )

  [thenSubj, elseSubj]

recycleBin = new Rx.Subject()

recycleBin
.subscribe (m) ->
  persistenceService.releaseLocalLock m
  stats.messageFinished m

[newMessages, restMessages]  = ifSplit allMessages, (m) ->
  persistenceService.checkAvaialbleForProcessingAndLockLocally m

restMessages.subscribe recycleBin

tolock = newMessages
.do (msg) ->
  stats.newMessage(msg)
.flatMap (msg) ->
  persistenceService.lockMessage msg

[lockedMessages, nakedMessages]  = ifSplit tolock, (m) ->
  m.locked is true

processedMessages = lockedMessages
.do (msg) ->
  stats.lockedMessage msg
.flatMap (msg) ->
  persistenceService.orderBySequenceNumber msg, recycleBin
.flatMap (msg) ->
  processMessage(messageProcessors, msg)

nakedMessages
.do (msg) ->
  stats.failedLock msg
.subscribe recycleBin

[successfulMessages, failedMessages]  = ifSplit processedMessages, (m) ->
  _.isArray m.processingResults

successfulMessages
.flatMap (m) ->
  persistenceService.unlockMessage m
.do (msg) ->
  stats.unlockedMessage msg
.subscribe recycleBin

failedMessages
.flatMap (m) ->
  persistenceService.reportMessageProcessingFailure m, m.processingResults
.subscribe recycleBin

initiateSelfDestructionSequence = ->
  Rx.Observable.interval(500).subscribe ->
    if stats.messagesInProgress() is 0
      console.info "Graceful exit", stats.get()
      process.exit 0

statsApp = express()

statsApp.get '/', (req, res) ->
  res.json stats.get()

statsApp.get '/stop', (req, res) ->
  res.send 'Ok'
  stats.panicMode = true
  initiateSelfDestructionSequence()

statsApp.listen 7777, ->
  console.log "Statistics is on port 7777"

Rx.Observable.interval(3000).subscribe ->
  console.info "+-------------------------------"
  console.info stats.get()
  console.info "+-------------------------------"