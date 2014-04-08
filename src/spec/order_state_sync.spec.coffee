Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
{SphereTestKit, SphereService} = require 'sphere-message-processing'

OrderStateSync = require '../lib/order_state_sync'
{config} = require '../config'

jasmine.getEnv().defaultTimeoutInterval = 30000

describe 'Order State Sync', ->
  statsOptions = () ->
    eventSubject: new Rx.ReplaySubject()
    offline: true

  testProject = "#{config.project_key}:#{config.client_id}:#{config.client_secret}"

  setupTestProject = (stats, requestQueue) ->
    SphereService.create stats,
      processorName: "test-kit"
      requestQueue: requestQueue
      connector:
        config:
          project_key: config.project_key
          client_id: config.client_id
          client_secret: config.client_secret
    .then (sphere) ->
      SphereTestKit.setupProject sphere

  verifyMasterProjectOrdersInState = (kit, stateKey, quantity) ->
    Q.all _.map(kit.orders, (o) -> kit.sphere.getOrderById(o.masterOrder.id))
    .then (masterOrders) ->
      kit.sphere.getStateByKey stateKey, 'LineItemState'
      .then (state) ->
        [masterOrders, state]
    .then ([masterOrders, state]) ->
      ps = _.map masterOrders, (masterOrder) ->
        found = _.find masterOrder.lineItems[0].state, (s) -> s.state.id is state.id and s.quantity is quantity

        if found then Q() else Q.reject("Order #{masterOrder.id} has line items in the wrong state!")

      Q.all ps

  it 'should replicate line item state transitions from retailer projects to one target master project', (done) ->
    processor = OrderStateSync().init statsOptions(),
      sourceProjects: testProject
      targetProject: testProject
      heartbeatInterval: 500
      logLevel: 'warn'

    setupTestProject processor.stats, processor.requestQueue
    .then (kit) ->
      _.reduce _.range(0, 6), ((p, c) -> (p.then -> kit.transitionRetailerOrderStates("A", kit.abcStateSwitch))), Q()
      .then -> kit
    .then (kit) ->
      d = Q.defer()

      subscription = processor.start _.map(kit.orders, (o)-> o.retailerOrder.id)
      .subscribe (event) ->
        stats = processor.stats.toJSON(true)

        if stats.processingErrors > 0
          subscription.dispose()
          d.reject "Messages processing failed"
        else if stats['LineItemStateTransition.processed'] is 30
          subscription.dispose()
          verifyMasterProjectOrdersInState kit, 'B', 20
          .then -> d.resolve()
          .fail (error) -> d.reject error
          .done()

      d.promise
    .then ->
      SphereTestKit.reportSuccess done, null, processor
    .fail (error) ->
      SphereTestKit.reportFailure done, error, null, processor
    .done()
