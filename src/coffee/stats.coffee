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
    @awaitOrderingIn = new Meter "awaitOrderingIn", @units
    @awaitOrderingOut = new Meter "awaitOrderingOut", @units
    @lockedMessages = new Meter "lockedMessages", @units
    @unlockedMessages = new Meter "unlockedMessages", @units
    @inProcessing = new Meter "inProcessing", @units
    @lockFailedMessages = new Meter "lockFailedMessages", @units
    @processedSuccessfully = new Meter "processedSuccessfully", @units
    @processingErrors = new Meter "processingErrors", @units

    @customStats = []
    @customMeters = []

  toJSON: (countOnly = false) ->
    json =
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
      inProcessing: if countOnly then @inProcessing.count() else @inProcessing.toJSON()
      lockFailedMessages: if countOnly then @lockFailedMessages.count() else @lockFailedMessages.toJSON()
      processingErrors: if countOnly then @processingErrors.count() else @processingErrors.toJSON()
      processedSuccessfully: if countOnly then @processedSuccessfully.count() else @processedSuccessfully.toJSON()
      panicMode: @panicMode
      paused: @paused


    _.each @customStats, (stat) ->
      json["#{stat.prefix}.#{stat.name}"] = stat.statJsonFn()

    _.each @customMeters, (meterDef) ->
      json["#{meterDef.prefix}.#{meterDef.name}"] = if countOnly then meterDef.count() else meterDef.toJSON()

    json

  addCustomStat: (prefix, name, statJsonFn) ->
    @customStats.push {prefix: prefix, name: name, statJsonFn: statJsonFn}
    this

  addCustomMeter: (prefix, name) ->
    meter = new Meter "inProcessing", @units
    @customMeters.push {prefix: prefix, name: name, meter: meter}
    meter

  applyBackpressureAtTick: (tick) ->
    @lastTick = tick
    @paused or @panicMode or @messagesInProgress() > 0

  messagesInProgress: () ->
    @messagesIn.count() - @messagesOut.count()

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

exports.Meter = Meter
exports.Stats = Stats