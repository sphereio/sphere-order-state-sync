Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
{util, MessageProcessing, SphereService, Repeater, ErrorStatusCode} = require 'sphere-message-processing'

p = MessageProcessing.builder()
.processorName "deliveryStatusUpdate"
.optimistExtras (o) ->
  o.describe('retryAttempts', 'Number of retries in case of optimistic locking conflicts.')
  .alias('retryAttempts', 10)
.messageCriteria 'resource(typeId="order" and id in ("05c5dacd-1ed4-4dd4-b7f2-4a1ae924371d","e2996b7b-32a8-4e87-8a3d-b9cbd611e9ef","aef7338f-078a-4cb0-837d-2601492f8bbf","8633c221-08c6-42e3-9413-d8c8095ccda0","7abc5039-16d9-4c49-b47b-5075dfe9db43"))'
.build()
.run (argv, stats, requestQueue) ->
  allowedAutoTransitionPaths = [
    {path: ['A', 'B', 'C']}
    {path: ['B', 'C', 'D'], condition: (sourceInfo, msg) -> true}
  ]

#  repeater = new Repeater {attempts: argv.retryAttempts}
  conflict = (e) -> e instanceof ErrorStatusCode and e.code is 409

  processDelivery = (sourceInfo, msg) ->
#    stateTransitionsPs = _.map msg.items, (deliveryItem) ->
#      order = msg.resource.obj
    Q({processed: true, processingResult: "Test"})


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
    Q({processed: true, processingResult: "Test"})

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
      Q({processed: true, processingResult: "Ignored"})