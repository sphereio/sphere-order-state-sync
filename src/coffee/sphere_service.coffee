Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
express = require 'express'
measured = require 'measured'
cache = require 'lru-cache'

class SphereService
  constructor: (@stats, options) ->

  getSourceInfo: () ->
    {name: "sphere"}

  getMessageSource: ->
    subject = new Rx.Subject()

    observable = subject
    .flatMap (tick) =>
      @_loadLatestMessages()

    [subject, observable]

  _loadLatestMessages: () ->
    # TODO
    return Rx.Observable.fromArray [
      {id: 3, resource: {typeId: "order", id: 1}, sequenceNumber: 3, name: "b"},
      {id: 1, resource: {typeId: "order", id: 1}, sequenceNumber: 1, name: "a"},
      {id: 2, resource: {typeId: "order", id: 1}, sequenceNumber: 2, name: "c"}]

  getLastProcessedSequenceNumber: (resource) ->
    # TODO
    Q(0)

  reportSuccessfullProcessing: (msg) ->
    console.info "Success: ", msg.payload
    # TODO
    Q(msg)

  reportMessageProcessingFailure: (msg, error, processor) ->
    console.error msg, error.stack
    # TODO
    Q(msg)

exports.SphereService = SphereService