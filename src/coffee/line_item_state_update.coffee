Q = require 'q'
{_} = require 'underscore'
fs = require 'q-io/fs'
http = require 'q-io/http'
_s = require 'underscore.string'
{MessageProcessing, SphereService, Repeater, ErrorStatusCode} = require 'sphere-message-processing'
util = require "./util"

p = MessageProcessing.builder()
.processorName "lineItemStateAndStockUpdate"
#.optimistDemand ['transitionConfig']
.optimistExtras (o) ->
  o.describe('retryAttempts', 'Number of retries in case of optimistic locking conflicts.')
  .describe('transitionConfig', 'The location of the configuration file that defines allowed transitions.')
  .describe('shippedStateKey', 'The key of the shipped state.')
  .default('retryAttempts', 10)
  .default('shippedStateKey', 'Shipped')
.messageCriteria 'resource(typeId="order")'
.build()
.run (argv, stats, requestQueue) ->
  Q.spread [util.loadFile(argv.transitionConfig)], (transitionConfigText) ->
    transitionConfig = JSON.parse transitionConfigText

    repeater = new Repeater {attempts: argv.retryAttempts}

    processDelivery = (sourceInfo, msg) ->
      Q.reject new Error("Delivery processing is not supported yet")

    processLineItemStateTransition = (sourceInfo, msg, log) ->
      sphere = sourceInfo.sphere
      order = msg.resource.obj

      sphere.getStateByRef msg.toState
      .then (state) ->
        if state.key isnt argv.shippedStateKey
          Q({processed: true, processingResult: {ignored: true}})
        else
          lineItem = _.find(order.lineItems, (li) -> li.id is msg.lineItemId)

          if not lineItem?
            Q.reject new Error("Line item with id #{msg.lineItemId} not found.")
          else
            sku = lineItem.variant.sku
            supplyChannelRef = lineItem.supplyChannel

            if not sku? or _s.isBlank(sku)
              Q.reject new Error("SKU is not defined on line item with id #{msg.lineItemId}.")
            else
              retries = 0

              repeater.execute
                recoverableError: (e) ->
                  if e instanceof ErrorStatusCode and e.code is 409
                    retries = retries + 1
                    log.push "retrying #{retries}"
                    true
                  else
                    false
                task: ->
                  log.push "getting inventory..."

                  sphere.getInvetoryEntryBySkuAndChannel sku, supplyChannelRef
                  .then (inventoryEntry) ->
                    log.push "got inventory #{inventoryEntry.id}@#{inventoryEntry.version}, available quantity: #{inventoryEntry.availableQuantity}. updating stock..."

                    if inventoryEntry.availableQuantity < msg.quantity
                      Q.reject new Error("not enough quantity in inventory entry with ID '#{inventoryEntry.id}' (available: #{inventoryEntry.availableQuantity}, needed: #{msg.quantity})")
                    else
                      sphere.removeInventoryQuantity inventoryEntry, msg.quantity
                      .then (updatedInventoryEntry) ->
                        log.push "updated inventory #{updatedInventoryEntry.id}@#{updatedInventoryEntry.version}, available quantity: #{updatedInventoryEntry.availableQuantity}."

                        {processed: true, processingResult: {id: inventoryEntry.id, retries: retries, quantity: msg.quantity, oldQuantity: inventoryEntry.availableQuantity, newQuantity: updatedInventoryEntry.availableQuantity, oldVersion: inventoryEntry.version, newVersion: updatedInventoryEntry.version}}

    processReturn = (sourceInfo, msg) ->
      Q.reject new Error("Return info processing is not supported yet")

    (sourceInfo, msg) ->
      transitionLineItemsState = sourceInfo.sphere.projectProps['transition']
      updateStock = sourceInfo.sphere.projectProps['stock']

      if msg.resource.typeId is 'order' and msg.type is 'DeliveryAdded' and transitionLineItemsState
        processDelivery sourceInfo, msg
      else if msg.resource.typeId is 'order' and msg.type is 'LineItemStateTransition' and updateStock
        log = []

        processLineItemStateTransition sourceInfo, msg, log
        .fail (error) ->
          Q.reject new Error("Error during stock update. Log: #{JSON.stringify log}. Cause: #{error.stack}")

      else if msg.resource.typeId is 'order' and msg.type is 'ReturnInfoAdded'
        processReturn sourceInfo, msg
      else
        Q({processed: true, processingResult: {ignored: true}})