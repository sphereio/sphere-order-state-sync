Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
express = require 'express'
measured = require 'measured'
cache = require 'lru-cache'

class MessageProcessor
  constructor: (@stats, options) ->
    @messageProcessors = options.processors # Array[(SourceInfo, Message) => Promise[Anything]]
    @messageSources = options.messageSources
    @heartbeatInterval = options.heartbeatInterval or 500

    @recycleBin = @_createRecycleBin()
    @errors = @_createErrorProcessor(@recycleBin)
    @unrecoverableErrors = @_createUnrecoverableErrorProcessor(@recycleBin)

  run: () ->
    heartbeat = Rx.Observable.interval @heartbeatInterval
    .filter (tick) =>
      not @stats.applyBackpressureAtTick(tick)

    messageSources = _.map @messageSources, (source) ->
      [sourceObserver, sourceObservable] = source.getMessageSource()
      heartbeat.subscribe sourceObserver
      sourceObservable

    all = Rx.Observable.merge messageSources
    .map (msg) =>
      @stats.incommingMessage msg

    maybeLocked = @_filterAndLockNewMessages(all)
    processed = @_doProcessMessages(maybeLocked)

    processed.subscribe @recycleBin

  _doProcessMessages: (messages) ->
    [locked, nonLocked, errors]  = @_split messages, (msg) ->
      Q(msg.lock?)

    # nothing can really go wrong here
    errors.subscribe @unrecoverableErrors

    nonLocked
    .do (msg) =>
      @stats.failedLock msg
    .subscribe @recycleBin

    locked
    .do (msg) =>
      @stats.lockedMessage msg
    .flatMap (msg) =>
      [sink, errors] = msg.persistence.orderBySequenceNumber msg, @recycleBin

      errors.subscribe @errors
      sink
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @_processMessage @messageProcessors, msg
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

      msg.persistence.reportSuccessfullProcessing msg
      .then (msg) ->
        subj.onNext(msg)
        subj.onCompleted()
      .fail (error) =>
        @errors.onNext {message: msg, error: error, processor: "Reporting successful processing"}
        subj.onCompleted()
      .done()

      subj
    .flatMap (msg) =>
      subj = new Rx.Subject()

      msg.persistence.unlockMessage msg
      .then (msg) ->
        subj.onNext(msg)
        subj.onCompleted()
      .fail (error) =>
        @errors.onNext {message: msg, error: error, processor: "Unlocking the message"}
        subj.onCompleted()
      .done()

      subj
    .do (msg) ->
      stats.unlockedMessage msg

  _processMessage: (processors, msg) ->
    try
      promises = _.map processors, (processor) ->
        processor msg.persistence.getSourceInfo(), msg.payload

      Q.all promises
    catch error
      Q.reject(error)

  _filterAndLockNewMessages: (messages) ->
    [newMessages, other, errors]  = @_split messages, (msg) ->
      msg.persistence.checkAvaialbleForProcessingAndLockLocally msg

    other.subscribe @recycleBin
    errors.subscribe @unrecoverableErrors

    newMessages
    .do (msg) =>
      @stats.newMessage(msg)
    .flatMap (msg) =>
      subj = new Rx.Subject()

      msg.persistence.lockMessage msg
      .then (maybeLocked) ->
        subj.onNext(maybeLocked)
        subj.onCompleted()
      .fail (error) =>
        @unrecoverableErrors.onNext {message: msg, error: error, "Locking the message"}
        subj.onCompleted()
      .done()

      subj

  _createRecycleBin: () ->
    recycleBin = new Rx.Subject()

    recycleBin
    .subscribe (msg) =>
      msg.persistence.releaseLocalLock msg
      @stats.messageFinished msg

    recycleBin

  _createErrorProcessor: (unrecoverableErrors, recycleBin) ->
    errorProcessor = new Rx.Subject()

    errorProcessor
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @stats.processingError msg
      msg.message.persistence.reportMessageProcessingFailure msg.message, msg.error, msg.processor
      .then (msg) ->
        subj.onNext msg
        subj.onCompleted()
      .fail (error) ->
        unrecoverableErrors.onNext {message: msg.message, error: error, processor: "Reporting processing error: #{msg.error.stack}"}
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

exports.MessageProcessor = MessageProcessor