Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
{util, MessageProcessing, SphereService, Repeater, ErrorStatusCode} = require 'sphere-message-processing'
util1 = require "./util"

module.exports = MessageProcessing.builder()
.processorName "orderStateUpdate"
.messageType 'order'
.messageExpand ['fromState', 'toState']
.build (argv, stats, requestQueue, cc, rootLogger) ->
  repeater = new Repeater {attempts: argv.retryAttempts}

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

  changeOrderStateIfNeeded = (sourceInfo, msg) ->
    sphere = sourceInfo.sphere

    Q.spread [
      sphere.getStateByRef(msg.fromState)
      sphere.getStateByRef(msg.toState),
      sphere.getCustomObjectForResource(msg.resource)
    ], (from, to, lineItemData) ->
      updateLineItemStateInCustomObject sphere, msg.resource.obj, from, to, msg, lineItemData
      .then (newLineItemData) ->
        [from, to, newLineItemData]
    .then ([from, to, lineItemData]) ->
      repeater.execute
        recoverableError: (e) -> e instanceof ErrorStatusCode and e.code is 409
        task: ->
          sphere.getOrderById msg.resource.id
          .then (order) ->
            if isFinishedOrder(order)
              {processed: true, processingResult: {ignored: true, reason: "Order is already in final state."}}
            else if isAllLineItemsCancelled(lineItemData)
              sphere.setOrderState order, "Cancelled"
              .then (newOrder) ->
                {processed: true, processingResult: {orderVersionAfterUpdate: newOrder.version, resolution: "cancelled"}}
            else if isAllLineItemsNonActionable(lineItemData)
              sphere.setOrderState order, "Complete"
              .then (newOrder) ->
                {processed: true, processingResult: {orderVersionAfterUpdate: newOrder.version, resolution: "complete"}}
            else
              {processed: true, processingResult: {ignored: true, reason: "Conditions not met"}}

  Q (sourceInfo, msg) ->
    if msg.resource.typeId is 'order' and msg.type is 'LineItemStateTransition'
      changeOrderStateIfNeeded(sourceInfo, msg)
    else
      Q({processed: true, processingResult: {ignored: true}})