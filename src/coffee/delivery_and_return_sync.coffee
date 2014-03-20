Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
{util, MessageProcessing, SphereService} = require 'sphere-message-processing'

p = MessageProcessing.builder()
.processorName "deliverySync"
.optimistUsage '--targetProject [PROJECT_CREDENTIALS]'
.optimistDemand ['targetProject']
.optimistExtras (o) ->
  o.describe('targetProject', 'Sphere.io credentials of the target project. Format: `prj-key:clientId:clientSecret`.')
  .alias('targetProject', 't')
.messageCriteria 'resource(typeId="order")'
.build()
.run (argv, stats, requestQueue) ->
  targetProject = util.parseProjectsCredentials argv.targetProject

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
    msg.resource.typeId is 'order' and msg.type in ['DeliveryAdded', 'ParcelAddedToDelivery']

  getTargetItemId = (sourceItemId, sourceOrder, targetOrder) ->
    lineItems = _.map sourceOrder.lineItems, (li, idx) -> {id: li.id, idx: idx, prop: "lineItems"}
    customLineItems = _.map sourceOrder.customLineItems, (li, idx) -> {id: li.id, idx: idx, prop: "customLineItems"}

    found = _.find lineItems.concat(customLineItems), (li) -> li.id is sourceItemId

    if found? and targetOrder[found.prop][found.idx]
      targetOrder[found.prop][found.idx].id
    else if found?
      throw new Error("Target order does not have correspondent (custom) line item #{JSON.stringify found}")
    else
      throw new Error("(custom) line item with ID not found! #{sourceItemId}")

  addDelivery = (sourceOrder, targetOrder, sourceDelivery) ->
    targetDeliveryItems = _.map sourceDelivery.items, (di) -> {id: getTargetItemId(di.id, sourceOrder, targetOrder), quantity: di.quantity}

    targetSphereService.addDelivery targetOrder, targetDeliveryItems
    .then (resOrder) ->
      {order: resOrder.id, version: resOrder.version, deliveryItems: targetDeliveryItems}

  addParcel = (sourceOrder, targetOrder, sourceDelivery, sourceParcel) ->
    targetDeliveryItems = _.sortBy _.map(sourceDelivery.items, (di) -> {id: getTargetItemId(di.id, sourceOrder, targetOrder), quantity: di.quantity}), (item) -> item.id

    if not targetOrder.shippingInfo?
      Q.reject new Error("Target order #{targetOrder.id} does not have any shippingInfo")
    else if not targetOrder.shippingInfo.deliveries?
      Q.reject new Error("Target order #{targetOrder.id} does not have any deliveries")
    else
      matchingDeliveries = _.filter targetOrder.shippingInfo.deliveries, (d) ->
        items = _.sortBy d.items, (item) -> item.id
        _.size(targetDeliveryItems) is _.size(items) and _.isEqual(targetDeliveryItems, items)

      if not matchingDeliveries? or _.isEmpty(matchingDeliveries)
        Q.reject new Error("Target order #{targetOrder.id} does not have matching delivery for source delivery #{sourceDelivery.id}")
      else if _.size(matchingDeliveries) > 1
        Q.reject new Error("Target order #{targetOrder.id} has more than one matching delivery! for source delivery #{JSON.stringify matchingDeliveries}")
      else
        targetSphereService.addParcel targetOrder, matchingDeliveries[0].id, sourceParcel.measurements, sourceParcel.trackingData
        .then (resOrder) ->
          {order: resOrder.id, version: resOrder.version, deliveryId: matchingDeliveries[0].id}

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
          addDelivery msg.resource.obj, targetOrder, msg.delivery
        else if msg.type is 'ParcelAddedToDelivery'
          addParcel msg.resource.obj, targetOrder, msg.delivery, msg.parcel
        else
          throw new Error("Unexpected message type #{msg.type}")
      .then (res) ->
        Q({processed: true, processingResult: res})