Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
{Rest} = require('sphere-node-connect')
util = require '../lib/util'
cache = require 'lru-cache'

class SphereService
  constructor: (@stats, options) ->
    @fetchHours = options.fetchHours
    @additionalMessageCriteria = options.additionalMessageCriteria
    @additionalMessageExpand = options.additionalMessageExpand or []
    @processorName = options.processorName
    @projectKey = options.connector.config.project_key
    @_client = new Rest options.connector
    @_messageFetchInProgress = false

    @referenceCacheOptions = options.referenceCacheOptions or {max: 1000, maxAge: 60 * 60 * 1000}
    @referenceCache = cache @referenceCacheOptions

    @requestMeter = @stats.addCustomMeter @projectKey, "requests"
    @requestTimer = @stats.addCustomTimer @projectKey, "requestTime"

    @stats.addCustomStat @projectKey, "referenceCacheSize", =>
      _.size @referenceCache

    @stats.cacheClearCommands.subscribe =>
      @referenceCache.reset()

  _get: (path) ->
    d = Q.defer()

    @requestMeter.mark()
    stopwatch = @requestTimer.start()
    @_client.GET path, (error, response, body) ->
      stopwatch.end()
      if error
        d.reject error
      else if response.statusCode is 200
        d.resolve body
      else
        d.reject new ErrorStatusCode(response.statusCode, body)

    d.promise

  _post: (path, json) ->
    d = Q.defer()

    @requestMeter.mark()
    stopwatch = @requestTimer.start()
    @_client.POST path, json, (error, response, body) ->
      stopwatch.end()
      if error
        d.reject error
      else if response.statusCode is 200 or response.statusCode is 201
        d.resolve body
      else
        d.reject new ErrorStatusCode(response.statusCode, body)

    d.promise

  _delete: (path) ->
    d = Q.defer()

    @requestMeter.mark()
    stopwatch = @requestTimer.start()
    @_client.DELETE path, (error, response, body) ->
      stopwatch.end()
      if error
        d.reject error
      else if response.statusCode is 200
        d.resolve body
      else
        d.reject new ErrorStatusCode(response.statusCode, body)

    d.promise

  getSourceInfo: () ->
    {name: "sphere.#{@projectKey}", prefix: @projectKey, sphere: this}

  # TODO: (optimization) implement pagging and remember the last error date so that amount of the messages can be reduced
  getMessageSource: ->
    subject = new Rx.Subject()

    observable = subject
    .flatMap =>
      @_loadLatestMessages()

    [subject, observable]

  getRecentMessages: (fromDate) ->
    additional = if @additionalMessageCriteria? then " and #{@additionalMessageCriteria}" else ""

    @_get @_pathWhere("/messages", "createdAt > \"#{util.formatDate fromDate}\"#{additional}", ["createdAt asc"], ["resource"].concat(@additionalMessageExpand), 0)
    .then (res) ->
      res.results

  _loadLatestMessages: () ->
    if @_messageFetchInProgress
      # if it takes too much time to get messages, then just ignore
      Rx.Observable.fromArray []
    else
      subj = new Rx.Subject()

      @_messageFetchInProgress = true

      @getRecentMessages util.addDateTime(new Date(), -1 * @fetchHours, 0, 0)
      .then (messages) ->
        _.each messages, (msg) ->
          subj.onNext msg
        subj.onCompleted()
      .fail (error) =>
        console.error "Error during message fetch!"
        console.error error.stack
        @stats.reportMessageFetchError()
      .finally =>
        @_messageFetchInProgress = false
      .done()

      subj

  getLastProcessedSequenceNumber: (resource) ->
    @_get "/custom-objects/#{@processorName}.lastSequenceNumber/#{resource.typeId}-#{resource.id}"
    .then (resp) ->
      resp.value
    .fail (error) ->
      if error instanceof ErrorStatusCode and (error.code is 404)
        0
      else
        throw error

  reportSuccessfullProcessing: (msg, lock, result) ->
    lastSnJson =
      container: "#{@processorName}.lastSequenceNumber"
      key: "#{msg.resource.typeId}-#{msg.resource.id}"
      value: msg.sequenceNumber

    @_post "/custom-objects", lastSnJson
    .then =>
      lock.value.state = "processed"
      lock.value.result = result if result?

      @_post "/custom-objects", lock

  reportMessageProcessingFailure: (msg, lock, error, processor) ->
    Q("updateLatSequenceNumber")
    .then =>
      lock.value.state = "error"
      lock.value.stage = processor
      lock.value.error = error.stack

      @_post "/custom-objects", lock

  _tryTakeLock: (id) ->
    json =
      container: "#{@processorName}.messages"
      key: "#{id}"
      version: 0
      value:
        state: "lockedForProcessing"

    @_post "/custom-objects", json
    .then (lock) ->
      {type: "new", payload: lock}
    .fail (error) =>
      if error instanceof ErrorStatusCode and (error.code is 409 or error.code is 500) # 500 because of the missing improvement
        # was just created by comeone
        @_get "/custom-objects/#{@processorName}.messages/#{id}"
        .then (lock) ->
          {type: "existing", payload: lock}
      else
        throw error

  _findMessageProcessingRecordOrLock: (id) ->
    @_get "/custom-objects/#{@processorName}.messages/#{id}"
    .then (lock) ->
      {type: "existing", payload: lock}
    .fail (error) =>
      if error instanceof ErrorStatusCode and error.code is 404
        @_tryTakeLock id
      else
        throw error

  lockMessage: (msg) ->
    @_findMessageProcessingRecordOrLock msg.id

  unlockMessage: (msg, lock) ->
    @_delete "/custom-objects/#{@processorName}.messages/#{msg.id}?version=#{lock.version}"

  _pathWhere: (path, where, sort = [], expand = [], limit = 100) ->
    sorting = if not _.isEmpty(sort) then "&" + _.map(sort, (s) -> "sort=" + encodeURIComponent(s)).join("&") else ""
    expanding = if not _.isEmpty(sort) then "&" + _.map(expand, (e) -> "expand=" + encodeURIComponent(e)).join("&") else ""

    "#{path}?where=#{encodeURIComponent(where)}#{sorting}#{expanding}&limit=#{limit}"

  ensureChannels: (defs) ->
    promises = _.map defs, (def) =>
      @_get @_pathWhere('/channels', "key=\"#{def.key}\"")
      .then (list) =>
        if list.total is 0
          json =
            key: def.key
            roles: def.roles
          @_post "/channels", json
        else
          list.results[0]
      .then (channel) ->
        channel.definition = def
        channel

    Q.all promises

  ensureTaxCategories: (defs) ->
    promises = _.map defs, (def) =>
      @_get @_pathWhere('/tax-categories', "name=\"#{def.name}\"")
      .then (list) =>
        if list.total is 0
          @_post "/tax-categories", def
        else
          list.results[0]
      .then (tc) ->
        tc.definition = def
        tc

    Q.all promises

  ensureStates: (defs) ->
    statePromises = _.map defs, (def) =>
      @_get @_pathWhere('/states', "key=\"#{def.key}\" and type=\"LineItemState\"")
      .then (list) =>
        if list.total is 0
          json =
            key: def.key
            type: 'LineItemState'
            initial: false
          @_post "/states", json
        else
          list.results[0]
      .then (state) ->
        state.definition = def
        state

    Q.all statePromises
    .then (createdStates) =>
      finalPromises = _.map createdStates, (state) =>
        if (not state.transitions? and state.definition.transitions?) or (state.transitions? and not state.definition.transitions?) or (state.transitions? and state.definition.transitions? and _.size(state.transitions) != _.size(state.definition.transitions))
          json =
            if state.definition.transitions?
              version: state.version
              actions: [{action: 'setTransitions', transitions: _.map(state.definition.transitions, (tk) -> {typeId: 'state', id: _.find(createdStates, (s) -> s.key is tk).id})}]
            else
              version: state.version
              actions: [{action: 'setTransitions'}]

          @_post "/states/#{state.id}", json
        else
          Q(state)

      Q.all finalPromises

  getFirstProduct: () ->
    @_get "/products?limit=1"
    .then (res) ->
      if res.total is 0
        throw new Error("No products in the project")
      else
        res.results[0]

  importOrder: (json) ->
    @_post "/orders/import", json

  updateOrderSyncSuatus: (order, channel, externalId) ->
    json =
      version: order.version
      actions: [{action: 'updateSyncInfo', channel: {typeId: "channel", id: channel.id}, externalId: externalId}]

    @_post "/orders/#{order.id}", json

  transitionLineItemState: (order, lineItemId, quantity, fromState, toState) ->
    json =
      version: order.version
      actions: [{action: 'transitionLineItemState', lineItemId: lineItemId, quantity: quantity, fromState: fromState, toState: toState}]

    @_post "/orders/#{order.id}", json

  _refCacheKey: (ref) ->
    ref.typeId + "-" + ref.id

  _keyCacheKey: (type, key) ->
    'keys' + '-' + type + '-' + key

  getChannelByRef: (ref) ->
    @_getCachedRef ref, (id) =>
      @_get "/channels/#{id}"

  getChannelByKey: (key) ->
    @_getCachedKey 'channel', key, (key) =>
      @_get @_pathWhere("/channels", "key=\"#{key}\"")
      .then (res) ->
        if res.total is not 1
          throw new Error("Channel with key: #{key} not found!")
        else
          res.results[0]

  getStateByKey: (key, type) ->
    @_getCachedKey 'state', type + '.' + key, (k) =>
      @_get @_pathWhere("/states", "key=\"#{key}\" and type=\"#{type}\"")
      .then (res) ->
        if res.total is not 1
          throw new Error("State with key #{key} of type #{type} not found!")
        else
          res.results[0]

  getStateByRef: (ref) ->
    @_getCachedRef ref, (id) =>
      @_get "/states/#{id}"

  _getCachedKey: (type, key, fetchFn) ->
    cached = @referenceCache.get @_keyCacheKey(type, key)

    if cached?
      Q(cached)
    else
      fetchFn(key)
      .then (res) =>
        @referenceCache.set @_keyCacheKey(type, key), res
        res

  _getCachedRef: (ref, fetchFn) ->
    cached = @referenceCache.get @_refCacheKey(ref)

    if ref.obj?
      @referenceCache.set @_refCacheKey(ref), ref.obj
      Q(ref.obj)
    else if cached?
      Q(cached)
    else
      fetchFn(ref.id)
      .then (res) =>
        @referenceCache.set @_refCacheKey(ref), res
        res

  getOrderById: (id) ->
    @_get "/orders/#{id}"

class ErrorStatusCode extends Error
  constructor: (@code, @body) ->
    @message = "Status code is #{@code}: #{JSON.stringify @body}"
    @name = 'ErrorStatusCode'
    Error.captureStackTrace this, this

exports.SphereService = SphereService