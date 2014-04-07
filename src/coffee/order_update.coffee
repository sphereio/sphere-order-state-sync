Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
{util, MessageProcessing, SphereService} = require 'sphere-message-processing'
util1 = require "./util"

p = MessageProcessing.builder()
.processorName "orderUpdate"
.optimistUsage '--url [URL]'
.optimistDemand ['url']
.optimistExtras (o) ->
  o.describe('url', 'Endpoint for updating an order.')
  .alias('url', 'u')
.messageCriteria 'resource(typeId="order")'
.build()
.run (argv, stats, requestQueue, cc) ->

  spherePromise = SphereService.create stats,
    sphereHost: argv.sphereHost
    requestQueue: requestQueue
    statsPrefix: "target."
    processorName: argv.processorName

  Q.spread [util1.loadFile(argv.transitionConfig), spherePromise], (transitionConfigText, targetSphereService) ->
    missingTransitions = if _s.isBlank(transitionConfigText) then [] else JSON.parse(transitionConfigText)

    supportedMessage = (msg) ->
      msg.resource.typeId is 'order' and msg.type is 'ParcelAddedToDelivery'

    orderUpdate = (sourceInfo, msg) ->
      if not supportedMessage(msg)
        Q({processed: true, processingResult: {ignored: true}})
      else
        console.log "msg: #{msg}"

    orderUpdate
