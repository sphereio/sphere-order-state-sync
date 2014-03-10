Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
{Rest} = require('sphere-node-connect')

class SphereService
  constructor: (@stats, options) ->
    @processorName = options.processorName
    @projectKey = options.connector.config.project_key
    @_client = new Rest options.connector

    @requestMeter = @stats.addCustomMeter @projectKey, "requests"

  _get: (path) ->
    d = Q.defer()

    @requestMeter.mark()
    @_client.GET path, (error, response, body) ->
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
    @_client.POST path, json, (error, response, body) ->
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
    @_client.DELETE path, (error, response, body) ->
      if error
        d.reject error
      else if response.statusCode is 200
        d.resolve body
      else
        d.reject new ErrorStatusCode(response.statusCode, body)

    d.promise

  getSourceInfo: () ->
    {name: "sphere.#{@projectKey}", prefix: @projectKey}

  getMessageSource: ->
    subject = new Rx.Subject()

    observable = subject
    .flatMap =>
      @_loadLatestMessages()

    [subject, observable]

  _loadLatestMessages: () ->
    # TODO
    return Rx.Observable.fromArray [
      {id: 3, resource: {typeId: "order", id: 1}, sequenceNumber: 3, name: "b"},
      {id: 1, resource: {typeId: "order", id: 1}, sequenceNumber: 1, name: "a"},
      {id: 2, resource: {typeId: "order", id: 1}, sequenceNumber: 2, name: "c"}]

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
      lock.value.stage = "processor"
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

class ErrorStatusCode extends Error
  constructor: (@code, @body) ->
    @message = "Status code is #{@code}: #{JSON.stringify @body}"
    @name = 'ErrorStatusCode'
    Error.captureStackTrace this, this


exports.SphereService = SphereService