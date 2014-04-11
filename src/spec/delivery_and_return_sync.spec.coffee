Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
{SphereTestKit, SphereService} = require 'sphere-message-processing'

DeliverySync = require '../lib/delivery_and_return_sync'
{config} = require '../config'

describe 'Delivery and Return Sync', ->
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

  verifyMasterProjectOrdersParcels = (kit, trackingId, carrier) ->
    Q.all _.map(kit.orders, (o) -> kit.sphere.getOrderById(o.masterOrder.id))
    .then (masterOrders) ->
      _.map masterOrders, (masterOrder) ->
        expect(_.size(masterOrder.shippingInfo.deliveries)).toEqual 1
        expect(_.size(masterOrder.shippingInfo.deliveries[0].parcels)).toEqual 1

        parcel = masterOrder.shippingInfo.deliveries[0].parcels[0]

        expect(parcel.trackingData.trackingId).toEqual trackingId
        expect(parcel.trackingData.carrier).toEqual carrier

  it 'should synchronize all deliveries and parcels to the target master project', (done) ->
    processor = DeliverySync().init statsOptions(),
      sourceProjects: testProject
      targetProject: testProject
      heartbeatInterval: 500
      logLevel: 'warn'

    setupTestProject processor.stats, processor.requestQueue
    .then (kit) ->
      kit.addSomeDeliveries().then -> kit
    .then (kit) ->
      d = Q.defer()

      subscription = processor.start _.map(kit.orders, (o) -> o.retailerOrder.id)
      .subscribe (event) ->
        stats = processor.stats.toJSON(true)

        if stats.processingErrors > 0
          subscription.dispose()
          d.reject "Messages processing failed"
        else if stats['DeliveryAdded.processed'] is 5 and stats['ParcelAddedToDelivery.processed'] is 5
          subscription.dispose()
          verifyMasterProjectOrdersParcels kit, "ABCD123", "DHL"
          .then -> d.resolve()
          .fail (error) -> d.reject error
          .done()

      d.promise
    .then ->
      SphereTestKit.reportSuccess done, null, processor
    .fail (error) ->
      SphereTestKit.reportFailure done, error, null, processor
    .done()
  , 30000
