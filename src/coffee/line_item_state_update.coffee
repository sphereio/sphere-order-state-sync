Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
{util, MessageProcessing, SphereService} = require 'sphere-message-processing'

p = MessageProcessing.builder()
.messageCriteria '(type="DeliveryAdded" or type="ReturnInfoAdded") and resource(typeId="order")'
.build()
.run (argv, stats, requestQueue) ->
  throw new Error("Not Implmented yet!")

  processDelivery: (sourceInfo, msg) ->
    console.log "Delivery %j", msg

  processReturn: (sourceInfo, msg) ->
    console.log "Return %j", msg

  (sourceInfo, msg) ->
    if msg.resource.typeId is 'order' and msg.type is 'DeliveryAdded'
      processDelivery sourceInfo, msg
    else if msg.resource.typeId is 'order' and msg.type is 'ReturnInfoAdded'
      processReturn sourceInfo, msg
    else
      Q({processed: true, processingResult: "Not interested"})