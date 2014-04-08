Q = require 'q'
{_} = require 'underscore'
fs = require 'q-io/fs'
http = require 'q-io/http'
_s = require 'underscore.string'
{MessageProcessing, SphereService, Repeater, ErrorStatusCode} = require 'sphere-message-processing'
util = require "./util"
nodemailer = require "nodemailer"

module.exports = MessageProcessing.builder()
.processorName "lineItemStateAndStockUpdate"
.optimistDemand ['smtpConfig', "emailFrom", "orderEmailSubject", "orderEmailTemplate"]
.optimistExtras (o) ->
  o.describe('retryAttempts', 'Number of retries in case of optimistic locking conflicts.')
  .describe('transitionConfig', 'The location of the configuration file that defines allowed transitions.')
  .describe('shippedStateKey', 'The key of the shipped state.')
  .describe('smtpConfig', 'SMTP Config JSON file: https://github.com/andris9/Nodemailer#setting-up-smtp')
  .describe('emailFrom', 'The sender of the emails.')
  .describe('orderEmailSubject', 'The sender of the emails.')
  .describe('orderEmailTemplate', 'The pathe to the email template: http://underscorejs.org/#template')
  .default('retryAttempts', 10)
  .default('shippedStateKey', 'Shipped')
.messageType 'order'
.build (argv, stats, requestQueue) ->
  Q.spread [util.loadFile(argv.transitionConfig), util.loadFile(argv.smtpConfig), util.loadFile(argv.orderEmailTemplate)], (transitionConfigText, smtpConfig, orderEmailTemplateText) ->
    transitionConfig = if transitionConfigText? and not _s.isBlank(transitionConfigText) then JSON.parse transitionConfigText else []

    repeater = new Repeater {attempts: argv.retryAttempts}
    smtp = nodemailer.createTransport "SMTP", JSON.parse(smtpConfig)
    orderEmailTemplate = _.template orderEmailTemplateText

    getApplicableTransitionPaths = (sphere, lineItem, quantity) ->
      ps = _.map lineItem.state, (s) ->
        sphere.getStateByRef s.state
        .then (state) ->
          {state: state, quantity: s.quantity}

      Q.all ps
      .then (listeItemStates) ->
        cfgs = _.filter transitionConfig, (cfg) ->
          _.find listeItemStates, (lis) ->
            firstKey = _.head cfg.path
            projectIsOk = not cfg.groups? or (sphere.projectProps['group']? and _.contains(cfg.groups, sphere.projectProps['group']))

            lis.state.key is firstKey and lis.quantity >= quantity and projectIsOk

        cfgs

    processDelivery = (sourceInfo, msg, log) ->
      # FIXME: implement this stuff
      return Q.reject new Error("LineItem state transition on delivery is not supported yet!")

      sphere = sourceInfo.sphere
      order = msg.resource.obj

      ps = _.map msg.delivery.items, (deliveryItem) ->
        lineItem = _.find order.lineItems, (li) -> li.id is deliveryItem.id

        getApplicableTransitionPaths sphere, lineItem, deliveryItem.quantity
        .then (paths) ->
          {lineItem: lineItem, paths: paths, quantity: deliveryItem.quantity}

      Q.all ps
      .then (lineItemsWithPaths) ->
        problematicLienItems = _.filter(lineItemsWithPaths, (li) ->_.isEmpty(li.paths))

        if not _.isEmpty(problematicLienItems)
          Q.reject new Error("Line items do not have enough quantity for the state transition: #{_.map(problematicLienItems, (li) -> lineItem.id)}")
        else
          missingStatesP = _.map missing.missing, (key) -> targetSphereService.getStateByKey(key, targetFromState.type)
          transitions =
            Q.all missingStatesP
            .then (missingStates) ->
              (_.reduce missingStates.concat(targetToState), ((acc, state) -> {curr: state, ts: acc.ts.concat({from: acc.curr, to: state})}), {curr: targetFromState, ts: []}).ts
        {processed: true, processingResult: res}

    processOrderImport = (sourceInfo, msg) ->
      res = Q.defer()
      emails = sourceInfo.sphere.projectProps['email']

      if not emails? or (_.isString(emails) and _s.isBlank(emails))
        emails = []
      else if _.isString(emails)
        emails = [emails]

      if not _.isEmpty(emails)
        mail =
          from: argv.smtpFrom
          to: emails.join(", ")
          subject: argv.orderEmailSubject.replace(/\%orderNumber\%/, msg.order.orderNumber or msg.order.id)
          text: orderEmailTemplate({order: msg.order})

        smtp.sendMail mail, (error, resp) ->
          if error
            res.reject error
          else
            res.resolve {processed: true, processingResult: {emails: emails}}
      else
        res.resolve {processed: true, processingResult: {ignored: true, reason: "no TO"}}

      res.promise

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
      sendEmail = sourceInfo.sphere.projectProps['email']

      log = []

      result =
        if msg.resource.typeId is 'order' and msg.type is 'DeliveryAdded' and transitionLineItemsState
          processDelivery sourceInfo, msg, log
        else if msg.resource.typeId is 'order' and msg.type is 'LineItemStateTransition' and updateStock
          processLineItemStateTransition sourceInfo, msg, log
        else if msg.resource.typeId is 'order' and msg.type is 'OrderImported' and sendEmail
          processOrderImport sourceInfo, msg
        else if msg.resource.typeId is 'order' and msg.type is 'ReturnInfoAdded'
          processReturn sourceInfo, msg
        else
          Q({processed: true, processingResult: {ignored: true}})

      result
      .fail (error) ->
        Q.reject new Error("Error! Log: #{JSON.stringify log}. Cause: #{error.stack}")