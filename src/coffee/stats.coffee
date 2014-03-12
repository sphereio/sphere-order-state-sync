Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
express = require 'express'
measured = require 'measured'

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
    @processor = options.processor
    @units = options.units or [{name: "Second", rateUnit: 1000}, {name: "Minute", rateUnit: 60 * 1000}]
    @started = new Date()
    @panicMode = false
    @paused = false
    @locallyLocked = 0
    @lastHeartbeat = -1

    @messagesIn = new Meter "messagesIn", @units
    @messagesOut = new Meter "messagesOut", @units
    @awaitOrderingIn = new Meter "awaitOrderingIn", @units
    @awaitOrderingOut = new Meter "awaitOrderingOut", @units
    @lockedMessages = new Meter "lockedMessages", @units
    @unlockedMessages = new Meter "unlockedMessages", @units
    @inProcessing = new Meter "inProcessing", @units
    @lockFailedMessages = new Meter "lockFailedMessages", @units
    @processedSuccessfully = new Meter "processedSuccessfully", @units
    @processingErrors = new Meter "processingErrors", @units
    @messageFetchErrors = new Meter "messageFetchErrors", @units
    @messageProcessingTimer = new measured.Timer()

    @customStats = []
    @customMeters = []
    @customTimers = []

    @_cacheClearCommandsObserver = new Rx.Subject()
    @cacheClearCommands = @_cacheClearCommandsObserver

    @_panicModeObserver = new Rx.Subject()
    @panicModeEvents = @_panicModeObserver

  toJSON: (countOnly = false) ->
    json =
      started: @started
      lastHeartbeat: @lastHeartbeat
      processor: @processor
      messagesIn: if countOnly then @messagesIn.count() else @messagesIn.toJSON()
      messagesOut: if countOnly then @messagesOut.count() else @messagesOut.toJSON()
      awaitOrderingIn: if countOnly then @awaitOrderingIn.count() else @awaitOrderingIn.toJSON()
      awaitOrderingOut: if countOnly then @awaitOrderingOut.count() else @awaitOrderingOut.toJSON()
      messagesInProgress: @messagesInProgress()
      messagesAwaiting: @messagesAwaiting()
      locallyLocked: @locallyLocked
      lockedMessages: if countOnly then @lockedMessages.count() else @lockedMessages.toJSON()
      unlockedMessages: if countOnly then @unlockedMessages.count() else @unlockedMessages.toJSON()
      inProcessing: if countOnly then @inProcessing.count() else @inProcessing.toJSON()
      lockFailedMessages: if countOnly then @lockFailedMessages.count() else @lockFailedMessages.toJSON()
      processingErrors: if countOnly then @processingErrors.count() else @processingErrors.toJSON()
      processedSuccessfully: if countOnly then @processedSuccessfully.count() else @processedSuccessfully.toJSON()
      messageFetchErrors: if countOnly then @messageFetchErrors.count() else @messageFetchErrors.toJSON()
      panicMode: @panicMode
      paused: @paused

    _.each @customStats, (stat) ->
      json["#{stat.prefix}.#{stat.name}"] = stat.statJsonFn()

    _.each @customMeters, (meterDef) ->
      json["#{meterDef.prefix}.#{meterDef.name}"] = if countOnly then meterDef.meter.count() else meterDef.meter.toJSON()

    if not countOnly
      json.messageProcessingTime = @messageProcessingTimer.toJSON().histogram

      _.each @customTimers, (timerDef) ->
        json["#{timerDef.prefix}.#{timerDef.name}"] = timerDef.timer.toJSON().histogram

    json

  addCustomStat: (prefix, name, statJsonFn) ->
    @customStats.push {prefix: prefix, name: name, statJsonFn: statJsonFn}
    this

  addCustomMeter: (prefix, name) ->
    meter = new Meter name, @units
    @customMeters.push {prefix: prefix, name: name, meter: meter}
    meter

  addCustomTimer: (prefix, name) ->
    timer = new measured.Timer()
    @customTimers.push {prefix: prefix, name: name, timer: timer}
    timer

  applyBackpressureAtTick: (tick) ->
    @lastHeartbeat = tick
    @paused or @panicMode or (@messagesInProgress() - @messagesAwaiting()) > 0

  messagesInProgress: () ->
    @messagesIn.count() - @messagesOut.count()

  messagesAwaiting: () ->
    @awaitOrderingIn.count() - @awaitOrderingOut.count()

  reportMessageFetchError: () ->
    @messageFetchErrors.mark()

  incommingMessage: (msg) ->
    @messagesIn.mark()
    msg

  messageProcessingStatered: (msg) ->
    @inProcessing.mark()

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

  startMessageProcessingTimer: () ->
    @messageProcessingTimer.start()

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

    statsApp.get '/clearCache', (req, res) =>
      @_cacheClearCommandsObserver.onNext "Doit!!!!"
      res.json
        message: 'Cache cleared!'

    statsApp.get '/stop', (req, res) =>
      res.json
        message: 'Self destruction sequence initiated!'

      @panicMode = true
      @_panicModeObserver.onNext "AAAAaaaAAAaaaa!!!!"
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
      console.log "Statistics is on port #{port}"

  startPrinter: (countOnly = false) ->
    Rx.Observable.interval(3000).subscribe =>
      console.info "+---------------- STATS ----------------+"
      console.info @toJSON(countOnly)
      console.info "+----------------- END -----------------+"

exports.Meter = Meter
exports.Stats = Stats