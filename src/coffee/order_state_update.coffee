Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
{util, MessageProcessing, SphereService, Repeater, ErrorStatusCode} = require 'sphere-message-processing'
util1 = require "./util"

module.exports = MessageProcessing.builder()
.processorName "orderStateUpdate"
.messageType 'order'
.optimistUsage '--targetProject [PROJECT_CREDENTIALS]'
.optimistExtras (o) ->
  o.describe('targetProject', 'Sphere.io credentials of the target project. Format: `prj-key:clientId:clientSecret`.')
  .alias('targetProject', 't')
.messageExpand ['fromState', 'toState']
.build (argv, stats, requestQueue, cc, rootLogger) ->
  repeater = new Repeater {attempts: argv.retryAttempts}

  spherePromise =
    if argv.targetProject
      targetProject = util.parseProjectsCredentials cc, argv.targetProject

      if _.size(targetProject) > 1
        throw new Error("Only one target project is allowed.")

      targetProject[0].user_agent = argv.processorName

      spherePromise = SphereService.create stats,
        sphereHost: argv.sphereHost
        requestQueue: requestQueue
        statsPrefix: "target."
        processorName: argv.processorName
        connector:
          config: targetProject[0]
    else
      Q(null)

  spherePromise.then (targetSphere) ->
    isFinishedOrder = (order) ->
      order.orderState is "Complete" or order.orderState is "Cancelled"

    isCanceledLineItemState = (state) ->
      state.key is 'canceled'

    isAllLineItemsCancelled = (lineItemData) ->
      foundNonCancelled = _.find lineItemData.value, (li, liId) ->
        li.CANCELLED_QUANTITY < li.TOTAL_QUANTITY

      not foundNonCancelled?

    isAllLineItemsNonActionable = (lineItemData) ->
      lineItemWithActionableState = _.find lineItemData.value, (li) ->
        _.some li, (q, state) -> state in ['Initial', 'picking', 'backorder', 'readyToShip', 'returned', 'returnApproved']

      completedLineItem = _.find lineItemData.value, (li) ->
        _.some li, (q, state) ->
          # BEWARE!!! This line assumes that line items have quantity 1. Removing this assumption from here would not be so easy.
          state in ['shipped', 'returnApproved', 'closed'] and (li.CANCELLED_QUANTITY < q)

      completedLineItem? and not lineItemWithActionableState?

    updateLineItemStateInCustomObject = (sphere, order, from, to, msg, data) ->
      if not data.value?
        data.value = {}

        # BEWARE!!! This initialization logic assumes that there is some kind of initial state and every line item gets it for it's whole quantity
        _.each order.lineItems, (li) ->
          data.value[li.id] = {}
          data.value[li.id][from.key] = li.quantity
          data.value[li.id].TOTAL_QUANTITY = li.quantity
          data.value[li.id].CANCELLED_QUANTITY = 0

      data.value[msg.lineItemId][from.key] = data.value[msg.lineItemId][from.key] - msg.quantity

      if data.value[msg.lineItemId][from.key] == 0
        delete data.value[msg.lineItemId][from.key]

      if not data.value[msg.lineItemId][to.key]?
        data.value[msg.lineItemId][to.key] = 0

      data.value[msg.lineItemId][to.key] = data.value[msg.lineItemId][to.key] + msg.quantity

      if isCanceledLineItemState(to)
        data.value[msg.lineItemId].CANCELLED_QUANTITY = data.value[msg.lineItemId].CANCELLED_QUANTITY + msg.quantity

      sphere.saveCustomObject data

    doUpdateOrder = (sphere, orderId, lineItemData) ->
      repeater.execute
        recoverableError: (e) -> e instanceof ErrorStatusCode and e.code is 409
        task: ->
          sphere.getOrderById orderId
          .then (order) ->
            if isFinishedOrder(order)
              {ignored: true, reason: "Order is already in final state."}
            else if isAllLineItemsCancelled(lineItemData)
              sphere.setOrderState order, "Cancelled"
              .then (newOrder) ->
                {orderVersionAfterUpdate: newOrder.version, resolution: "cancelled"}
            else if isAllLineItemsNonActionable(lineItemData)
              sphere.setOrderState order, "Complete"
              .then (newOrder) ->
                {orderVersionAfterUpdate: newOrder.version, resolution: "complete"}
            else
              {ignored: true, reason: "Conditions not met"}

    getMasterOrderId = (sphere, msg) ->
      if sphere?
        masterSyncInfosP = _.map msg.resource.obj.syncInfo, (si) ->
          sphere.getChannelByRef(si.channel)
          .then (ch) ->
            {channel: ch, syncInfo: si}

        Q.all masterSyncInfosP
        .then (masterSyncInfos) ->
          masterSyncInfo = _.find masterSyncInfos, (i) -> i.channel.key is 'master'

          if not masterSyncInfo?
            throw new Error("Sync Info with master order id is not found for the order: #{msg.resource.id}.")
          else
            masterSyncInfo.syncInfo.externalId
      else
        Q(null)

    changeOrderStateIfNeeded = (sourceInfo, msg) ->
      sphere = sourceInfo.sphere

      getMasterOrderId(targetSphere, msg)
      .then (masterOrderId) ->
        Q.spread [
          sphere.getStateByRef(msg.fromState)
          sphere.getStateByRef(msg.toState)
          sphere.getCustomObjectForResource(msg.resource)
        ], (from, to, lineItemData) ->
          updateLineItemStateInCustomObject sphere, msg.resource.obj, from, to, msg, lineItemData
          .then (newLineItemData) ->
            [from, to, newLineItemData]
        .then ([from, to, lineItemData]) ->
          sourcePromise = doUpdateOrder sphere, msg.resource.id, lineItemData
          targetPromise =
            if targetSphere and masterOrderId
              doUpdateOrder(targetSphere, masterOrderId, lineItemData)
            else
              Q({ignored: true, reason: "no target"})

          Q.spread [sourcePromise, targetPromise], (sourceRes, targetRes) ->
            {processed: true, processingResult: {ignored: sourceRes.ignored and targetRes.ignored, source: sourceRes, target: targetRes}}

    (sourceInfo, msg) ->
      if msg.resource.typeId is 'order' and msg.type is 'LineItemStateTransition'
        changeOrderStateIfNeeded(sourceInfo, msg)
      else
        Q({processed: true, processingResult: {ignored: true}})