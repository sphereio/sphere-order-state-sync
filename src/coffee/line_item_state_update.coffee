Q = require 'q'
{_} = require 'underscore'
fs = require 'q-io/fs'
http = require 'q-io/http'
_s = require 'underscore.string'
{MessageProcessing, SphereService, Repeater, ErrorStatusCode} = require 'sphere-message-processing'
util = require "./util"

p = MessageProcessing.builder()
.processorName "lineItemStateAndStockUpdate"
.optimistDemand ['transitionConfig']
.optimistExtras (o) ->
  o.describe('transitionConfig', 'The location of the configuration file that defines allowed transitions.')
.messageCriteria 'resource(typeId="order")'
.build()
.run (argv, stats, requestQueue) ->
  Q.spread [util.loadFile(argv.transitionConfig)], (transitionConfigText) ->
    transitionConfig = JSON.parse transitionConfigText

    processDelivery = (sourceInfo, msg) ->
      order = msg.resource.obj

      stateTransitionsPs = _.map msg.items, (deliveryItem) ->

      Q({processed: true, processingResult: {ignored: true}})


    processLineItemStateTransition = (sourceInfo, msg) ->

  #      Q.all stateTransitionsPs
  #      .then (stateTransitionsResults) ->
  #      if processStock
  #        # TODO: aggregate stock updates by SKU -> quantity
  #        stockUpdatePs = _.map msg.items, (deliveryItem) ->
  #          repeater.execute
  #            recoverableError: conflict
  #            task: ->
  #              # TODO: reduce stock for the line item SKUs
  #              console.log "Delivery %j", msg
  #              Q({processed: true, processingResult: "WIP"})
  #
  #        Q.all stockUpdatePs
  #        .then (res) ->
  #            {stateTransitionsResults: stateTransitionsResults, stockUpdateResults: res}
  #      else
  #        Q({stateTransitionsResults: stateTransitionsResults})
  #    .then (res) ->
  #        console.log "Delivery ", res
  #        {processed: true, processingResult: "WIP"}
      Q({processed: true, processingResult: {ignored: true}})

    processReturn = (sourceInfo, msg) ->
      Q.reject new Error("Return info processing is not supported yet")

    (sourceInfo, msg) ->
      updateStock = sourceInfo.sphere.projectProps['+stock']

      if msg.resource.typeId is 'order' and msg.type is 'DeliveryAdded'
        processDelivery sourceInfo, msg
      else if msg.resource.typeId is 'order' and msg.type is 'LineItemStateTransition' and updateStock
        processLineItemStateTransition sourceInfo, msg
      else if msg.resource.typeId is 'order' and msg.type is 'ReturnInfoAdded'
        processReturn sourceInfo, msg
      else
        Q({processed: true, processingResult: {ignored: true}})