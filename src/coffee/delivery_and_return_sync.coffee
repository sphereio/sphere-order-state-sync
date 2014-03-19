Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
{util, MessageProcessing, SphereService} = require 'sphere-message-processing'

p = MessageProcessing.builder()
.optimistUsage '--targetProject [PROJECT_CREDENTIALS]'
.optimistDemand ['targetProject']
.optimistExtras (o) ->
  o.describe('targetProject', 'Sphere.io credentials of the target project. Format: `prj-key:clientId:clientSecret`.')
  .alias('targetProject', 't')
.messageCriteria '(type in ("DeliveryAdded", "ParcelAddedToDelivery", "ReturnInfoAdded") and resource(typeId="order")'
.build()
.run (argv, stats, requestQueue) ->
  throw new Error("Not Implmented yet!")

  targetProject = util.parseProjectsCredentials argv.sourceProjects

  if _.size(targetProject) > 1
    throw new Error("Only one target project is allowed.")

  targetProject[0].user_agent = argv.processorName

  targetSphereService = new SphereService stats,
    sphereHost: argv.sphereHost
    requestQueue: requestQueue
    statsPrefix: "target."
    processorName: argv.processorName
    connector:
      config: targetProject[0]

  supportedMessage = (msg) ->
    msg.resource.typeId is 'order' and msg.type in ['DeliveryAdded', 'ParcelAddedToDelivery', 'ReturnInfoAdded']

  getTargetItemId = (sourceItemId, sourceOrder, targetOrder) ->
    lineItems = _.map sourceOrder.lineItems, (li, idx) -> {id: li.id, idx: idx, prop: "lineItems"}
    customLineItems = _.map sourceOrder.customLineItems, (li, idx) -> {id: li.id, idx: idx, prop: "customLineItems"}

    found = _.find lineItems.concat(customLineItems), (li) -> li is sourceItemId

    if found? and targetOrder[found.prop][found.idx]
      targetOrder[found.prop][found.idx].id
    else if found?
      throw new Erorr("Target order does not have correspondent (custom) line item #{JSON.stringify found}")
    else
      throw new Erorr("(custom) line item with ID not found! #{sourceItemId}")

  addDelivery = (sourceOrder, targetOrder, sourceDeliveryItems) ->
    targetDeliveryItems = _.map sourceDeliveryItems, (di) -> {id: getTargetItemId(di.id), quantity: di.quantity}

    targetSphereService.addDelivery targetOrder, targetDeliveryItems
    .then (resOrder) ->
      {order: resOrder.id, deliveryItems: targetDeliveryItems}

  (sourceInfo, msg) ->
    if not supportedMessage(msg)
      Q({processed: true, processingResult: "Not interested"})
    else
      masterSyncInfosP = _.map msg.resource.obj.syncInfo, (si) ->
        sourceInfo.sphere.getChannelByRef(si.channel)
        .then (ch) ->
          {channel: ch, syncInfo: si}

      Q.all masterSyncInfosP
      .then (masterSyncInfos) ->
        masterSyncInfo = _.find masterSyncInfos, (i) -> i.channel.key is 'master'

        if not masterSyncInfo?
          throw new Error("Sync Info with master order id is not found for the order: #{msg.resource.id}.")
        else
          targetSphereService.getOrderById masterSyncInfo.syncInfo.externalId
      .then (targetOrder) ->
        if msg.type is 'DeliveryAdded'
          addDelivery msg.resource.obj, targetOrder, msg.delivery.items
        else if msg.type is 'ParcelAddedToDelivery'
          throw new Error("TODO: not implmennted yet")
        else if msg.type is 'ParcelAddedToDelivery'
          throw new Error("TODO: not implmennted yet")
        else
          throw new Error("Unexpected message type #{msg.type}")
      .then (res) ->
        Q({processed: true, processingResult: res})