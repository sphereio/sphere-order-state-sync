Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'

class  SphereTestKit
  stateDefs: [
    {key: "A", transitions: ["B"]}
    {key: "B", transitions: ["C", "D"]}
    {key: "C", transitions: ["D"]}
    {key: "D", transitions: ["E"]}
    {key: "E", transitions: ["A"]}
  ]

  channelDefs: [
    {key: 'master', roles: ['OrderImport']}
  ]

  taxCategoryDefs: [
    {name: 'Test Category', rates: [{name: "Test Rate", amount: 0.19, includedInPrice: false, country: 'DE'}]}
  ]

  constructor: (@sphere) ->

  setupProject: ->
    Q.all [
      @configureStates()
      @configureChannels()
      @configureProduct()
      @configureTaxCategory()
    ]
    .then =>
      orders = _.map _.range(1, 6), (idx) =>
        @createTestOrder idx

      Q.all orders
    .then (orders) =>
      @orders = _.map orders, (os) ->
        [m, r] = os
        {retailerOrder: r, masterOrder: m}

      console.info "Orders"
      _.each @orders, (o, i) ->
        console.info i, "Retailer: #{o.retailerOrder.id}, Master: #{o.masterOrder.id}"
      console.info _.map(@orders, (o)-> "\"#{o.retailerOrder.id}\"").join(',')
      console.info "\n"

      console.info "Project setup finished"
      this

  ref: (type, obj) ->
    {typeId: type, id: obj.id}

  stateByKey: (key) ->
    _.find @states, (s) -> s.key == key

  stateById: (id) ->
    _.find @states, (s) -> s.id == id

  transitionRetailerOrderStates: () ->
    ps = _.map @orders, (os) =>
      currStates = _.filter os.retailerOrder.lineItems[0].state, (s) => s.state.id != @initialState.id

      p = if _.isEmpty(currStates)
        @sphere.transitionLineItemState os.retailerOrder, os.retailerOrder.lineItems[0].id, 20, @ref('state', @initialState), @ref('state', @stateByKey('A'))
      else
        currStateId = currStates[0].state.id
        currStateQ = currStates[0].quantity

        newState = switch @stateById(currStateId).key
          when'A' then @stateByKey 'B'
          when'B' then @stateByKey 'D'
          when'D' then @stateByKey 'E'
          when'E' then @stateByKey 'A'
          else throw new Error("Unsupported state #{@stateById(currStateId).key}")

        @sphere.transitionLineItemState os.retailerOrder, os.retailerOrder.lineItems[0].id, currStateQ, @ref('state', {id: currStateId}), @ref('state', newState)

      p
      .then (newOrder) ->
        os.retailerOrder = newOrder
        newOrder
    Q.all ps

  scheduleStateTransitions: () ->
    Rx.Observable.interval 2000
    .subscribe =>
      @transitionRetailerOrderStates()
      .then ->
        console.info "Transition finished"
      .fail (error) ->
        console.error "Error during state transition"
        console.error error
      .done()

  configureStates: ->
    Q.all [
      @sphere.ensureStates [{key: "Initial"}]
      @sphere.ensureStates @stateDefs
    ]
    .then (states) =>
      [[@initialState], @states] = states
      console.info "States configured"
      [@initialState, @states]

  configureChannels: ->
    @sphere.ensureChannels @channelDefs
    .then (channels) =>
      [@masterChannel] = channels
      console.info "Channels configured"
      @masterChannel

  configureProduct: () ->
    @sphere.getFirstProduct()
    .then (product) =>
      @product = product
      console.info "Product found"
      product

  configureTaxCategory: () ->
    @sphere.ensureTaxCategories @taxCategoryDefs
    .then (tc) =>
      [@taxCategory] = tc
      console.info "Tax category configured"
      tc

  _orderJson: () ->
    {
      lineItems: [{
        variant: {
          sku: @product.masterData.staged.masterVariant.sku
        },
        quantity: 30,
        taxRate: {
          name: "some_name",
          amount: 0.19,
          includedInPrice: true,
          country: "US",
          id: @taxCategory.id
        },
        name: {
          en: "Some Product"
        },
        price: {
          country: "US",
          value: {
            centAmount: 1190,
            currencyCode: "USD"
          }
        }
      }],
      totalPrice: {
        currencyCode: "USD",
        centAmount: 1190
      },
      shippingAddress: {
        country: "US"
      },
      taxedPrice: {
        taxPortions: [{
          amount: {
            centAmount: 190,
            currencyCode: "USD"
          },
          rate: 0.19
        }],
        totalGross: {
          centAmount: 1190,
          currencyCode: "USD"
        },
        totalNet: {
          centAmount: 1000,
          currencyCode: "USD"
        }
      }
    }

  createTestOrder: (idx) ->
    Q.all [
      @sphere.importOrder @_orderJson()
      @sphere.importOrder @_orderJson()
    ]
    .then (orders) =>
      [masterOrder, retailerOrder] = orders

      @sphere.updateOrderSyncSuatus retailerOrder, @masterChannel, masterOrder.id
      .then (newRetailerOrder) ->
        [masterOrder, newRetailerOrder]

  @run: (sphereService) ->
    sphereTestKit = new SphereTestKit sphereService
    sphereTestKit.setupProject()
    .then (kit) ->
      console.info "Done"

      kit.scheduleStateTransitions()
    #  sphereService.getRecentMessages(util.addDateTime(new Date(), -3, 0, 0))
    .then (foo) ->
      console.info _.size(foo)
      console.info foo[0]
    .fail (error) ->
      console.error "Errror during setup"
      console.error error.stack
    .done()

exports.SphereTestKit = SphereTestKit
