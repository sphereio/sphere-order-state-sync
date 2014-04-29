Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
{SphereTestKit, SphereService} = require 'sphere-message-processing'

OrderStateUpdate = require '../lib/order_state_update'
{config} = require '../config'

describe 'Order State Update', ->
  statsOptions = () ->
    eventSubject: new Rx.ReplaySubject()
    offline: true

  testProject = "#{config.project_key}:#{config.client_id}:#{config.client_secret}"

  setupTestProject = (stats, requestQueue, onlyOneOrderWith3LineItems = true) ->
    SphereService.create stats,
      processorName: "test-kit"
      requestQueue: requestQueue
      connector:
        config:
          project_key: config.project_key
          client_id: config.client_id
          client_secret: config.client_secret
    .then (sphere) ->
      SphereTestKit.setupProject sphere, onlyOneOrderWith3LineItems

  orZero = (x) ->
    if x? then x else 0

  performTest = (done, finalOrderState, lineItemTransitions) ->
    processor = OrderStateUpdate().init statsOptions(),
      sourceProjects: testProject
      heartbeatInterval: 500
      logLevel: 'warn'

    setupTestProject processor.stats, processor.requestQueue
    .then (kit) ->
      reduceFn = (acc, transition) ->
        acc.then ->
          kit.transitionStatePath(transition.lineItemIdx, transition.quantity, transition.path)

      _.reduce lineItemTransitions, reduceFn, Q()
      .then -> kit
    .then (kit) ->
      d = Q.defer()

      subscription = processor.start [kit.order.id]
      .subscribe (event) ->
        stats = processor.stats.toJSON(true)
        expectedProcessedCount = _.reduce lineItemTransitions, ((acc, t) -> acc + (_.size(t.path) - 1)), 0

        if stats.processingErrors > 0
          subscription.dispose()
          d.reject "Messages processing failed"
        else if (orZero(stats['LineItemStateTransition.processed']) + orZero(stats['LineItemStateTransition.ignored'])) is expectedProcessedCount
          subscription.dispose()

          kit.sphere.getOrderById(kit.order.id)
          .then (order) ->
            if order.orderState is finalOrderState
              d.resolve()
            else
              d.reject new Error("Order is expected to be in state '#{finalOrderState}', but it is in state '#{order.orderState}'!")
          .fail (error) ->
            d.reject error
          .done()

      d.promise
    .then ->
      SphereTestKit.reportSuccess done, null, processor
    .fail (error) ->
      SphereTestKit.reportFailure done, error, null, processor
    .done()

  it 'should set order state to complete if line items are in the right state', (done) ->
    performTest done, 'Complete', [
      {lineItemIdx: 0, quantity: 30, path: ['Initial', 'picking', 'shipped', 'closed']}
      {lineItemIdx: 1, quantity: 30, path: ['Initial', 'picking', 'shipped']}
      {lineItemIdx: 2, quantity: 30, path: ['Initial', 'picking', 'canceled', 'closed']}
    ]
  , 60000

  it 'should not set order state to back to open after completeon', (done) ->
    performTest done, 'Complete', [
      {lineItemIdx: 0, quantity: 30, path: ['Initial', 'picking', 'shipped', 'closed']}
      {lineItemIdx: 1, quantity: 30, path: ['Initial', 'picking', 'shipped']}
      {lineItemIdx: 2, quantity: 30, path: ['Initial', 'picking', 'canceled', 'closed']}
      {lineItemIdx: 0, quantity: 30, path: ['closed', 'picking']}
      {lineItemIdx: 1, quantity: 30, path: ['shipped', 'picking']}
      {lineItemIdx: 2, quantity: 30, path: ['closed', 'backorder']}
    ]
  , 60000

  it 'should keep order state in open if some line items are still in processing', (done) ->
    performTest done, 'Open', [
      {lineItemIdx: 0, quantity: 30, path: ['Initial', 'picking', 'shipped', 'closed']}
      {lineItemIdx: 1, quantity: 30, path: ['Initial', 'picking', 'shipped']}
      {lineItemIdx: 2, quantity: 20, path: ['Initial', 'backorder']}
      {lineItemIdx: 2, quantity: 10, path: ['Initial', 'picking', 'shipped']}
    ]
  , 60000

  it 'should set order state to cancelled if all line items were in cancelled state', (done) ->
    performTest done, 'Cancelled', [
      {lineItemIdx: 0, quantity: 30, path: ['Initial', 'picking', 'canceled', 'closed']}
      {lineItemIdx: 1, quantity: 30, path: ['Initial', 'canceled']}
      {lineItemIdx: 2, quantity: 10, path: ['Initial', 'canceled']}
      {lineItemIdx: 2, quantity: 20, path: ['Initial', 'backorder', 'canceled', 'closed']}
    ]
  , 60000

  it 'should set order state to cancelled if all line items were in cancelled and dont changed order state afterwards', (done) ->
    performTest done, 'Cancelled', [
      {lineItemIdx: 0, quantity: 30, path: ['Initial', 'picking']}
      {lineItemIdx: 1, quantity: 30, path: ['Initial', 'canceled']}
      {lineItemIdx: 2, quantity: 30, path: ['Initial', 'backorder', 'canceled', 'closed']}
      {lineItemIdx: 0, quantity: 30, path: ['picking', 'canceled', 'closed']}
      {lineItemIdx: 1, quantity: 30, path: ['canceled', 'shipped']}
      {lineItemIdx: 2, quantity: 30, path: ['closed', 'shipped']}
      {lineItemIdx: 0, quantity: 30, path: ['closed', 'shipped']}
    ]
  , 60000

  it 'should update state in master and retailer if configured so', (done) ->
    processor = OrderStateUpdate().init statsOptions(),
      sourceProjects: testProject
      targetProject: testProject
      heartbeatInterval: 500
      logLevel: 'warn'

    setupTestProject processor.stats, processor.requestQueue, false
    .then (kit) ->
      kit.order = kit.orders[0].retailerOrder

      kit.transitionStatePath 0, 30, ['Initial', 'picking', 'shipped', 'closed']
      .then -> kit
    .then (kit) ->
      d = Q.defer()

      subscription = processor.start [kit.orders[0].retailerOrder.id]
      .subscribe (event) ->
        stats = processor.stats.toJSON(true)

        if stats.processingErrors > 0
          subscription.dispose()
          d.reject "Messages processing failed"
        else if (orZero(stats['LineItemStateTransition.processed']) + orZero(stats['LineItemStateTransition.ignored'])) is 3
          subscription.dispose()

          Q.spread [
            kit.sphere.getOrderById(kit.orders[0].retailerOrder.id)
            kit.sphere.getOrderById(kit.orders[0].masterOrder.id)
          ], (retailerOrder, masterOrder) ->
            if retailerOrder.orderState is 'Complete' and masterOrder.orderState is 'Complete'
              d.resolve()
            else
              d.reject new Error("Order is expected to be in state 'Complete'! Actial Retailer: '#{retailerOrder.orderState}', master: '#{masterOrder.orderState}'.")
          .fail (error) ->
            d.reject error
          .done()

      d.promise
    .then ->
      SphereTestKit.reportSuccess done, null, processor
    .fail (error) ->
      SphereTestKit.reportFailure done, error, null, processor
    .done()
  , 60000