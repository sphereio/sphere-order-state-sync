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
    @errors = @_createErrorProcessor @recycleBin
    @unrecoverableErrors = @_createUnrecoverableErrorProcessor @recycleBin

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

    locked = @_filterAndLockNewMessages all

    @_doProcessMessages locked
    .subscribe @_ignoreCopleted(@recycleBin)

  _doProcessMessages: (lockedMessages) ->
    toUnlock = new Rx.Subject()

    processed = lockedMessages
    .do (msg) =>
      @stats.messageProcessingStatered msg
    .flatMap (msg) =>
      [sink, errors] = msg.persistence.orderBySequenceNumber msg, toUnlock

      errors.subscribe @_ignoreCopleted(@errors)
      sink
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @_processMessage @messageProcessors, msg
      .then (results) =>
        if not _.isEmpty(_.find(results, (res) -> res.processed))
          msg.result = _.map results, (res) -> res.processingResult
          subj.onNext(msg)
        else
          # unprocessed messages are not allowed! If message should be ignored, then it should be done explicitly in processor
          @errors.onNext {message: msg, error: new Error("No processor is defined, that can handle this message."), processor: "Actual processing"}

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

    Rx.Observable.merge [processed, toUnlock]
    .flatMap (msg) =>
      subj = new Rx.Subject()

      msg.persistence.unlockMessage msg
      .then (msg) ->
        subj.onNext msg
        subj.onCompleted()
      .fail (error) =>
        @errors.onNext {message: msg, error: error, processor: "Unlocking the message"}
        subj.onCompleted()
      .done()

      subj

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

    other.subscribe @_ignoreCopleted(@recycleBin)
    errors.subscribe @_ignoreCopleted(@unrecoverableErrors)

    newMessages
    .flatMap (msg) =>
      [locked, errors, toRecycle] = msg.persistence.lockMessage msg

      errors.subscribe @_ignoreCopleted(@unrecoverableErrors)
      toRecycle.subscribe @_ignoreCopleted(@recycleBin)

      locked

  _ignoreCopleted: (observer) ->
    Rx.Observer.create(
      (next) -> observer.onNext next,
      (error) -> observer.onError error,
      () ->
    )


  _createRecycleBin: () ->
    recycleBin = new Rx.Subject()

    nextFn = (msg) =>
      msg.persistence.releaseLocalLock msg
      @stats.messageFinished msg
    errorFn = (error) ->
      console.error error.stack
    completeFn = ->
      console.error "Recycle Bin completed! This should never happen!!"
      console.error new Error().stack

    recycleBin
    .subscribe Rx.Observer.create(nextFn, errorFn, completeFn)

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

  _createUnrecoverableErrorProcessor: (recycleBin) ->
    errorProcessor = new Rx.Subject()

    errorProcessor
    .flatMap (box) ->
      # TODO
      console.error "Error during: #{box.processor}"
      console.error box.error.stack
    .subscribe recycleBin

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