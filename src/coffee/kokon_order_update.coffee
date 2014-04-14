Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
{util, MessageProcessing, SphereService} = require 'sphere-message-processing'
util1 = require "./util"

module.exports = MessageProcessing.builder()
.processorName "kokonOrderUpdate"
.optimistUsage '--url [URL]'
.optimistDemand ['url']
.optimistExtras (o) ->
  o.describe('url', 'Endpoint for updating trackingId.')
  .alias('u', 'u')
.messageType 'order'
.build (argv, stats, requestQueue, cc) ->

  supportedMessage = (msg) ->
    msg.resource.typeId is 'order' and msg.type is 'ParcelAddedToDelivery'

  process = (sourceInfo, msg) ->
    if not supportedMessage(msg)
      Q({processed: true, processingResult: {ignored: true}})
    else
      console.log "Parcel added."
      Q({processed: true, processingResult: {ignored: true}})

  process
