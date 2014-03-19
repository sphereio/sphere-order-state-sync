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
.messageCriteria 'type="LineItemStateTransition" and resource(typeId="order")'
.messageExpand ['fromState', 'toState']
.build()
.run (argv, stats, requestQueue) ->
  targetProject = util.parseProjectsCredentials argv.sourceProjects

  if _.size(targetProject) > 1
    throw new Error("Only one target project is allowed.")

  missingTransitions = [
    {from: 'B', to: 'D', missing: ['C']}
  ]

  targetProject[0].user_agent = argv.processorName

  targetSphereService = new SphereService stats,
    requestQueue: requestQueue
    statsPrefix: "target."
    processorName: argv.processorName
    connector:
      config: targetProject[0]

  supportedMessage = (msg) ->
    msg.resource.typeId is 'order' and msg.type is 'LineItemStateTransition'

  doTargetSateTransition = (targetOrder, targetLineItemId, quantity, targetFromState, targetToState, date) ->
    missing = _.find missingTransitions, (m) ->
      m.from is targetFromState.key and m.to is targetToState.key

    transitions =
      if missing?
        missingStatesP = _.map missing.missing, (key) -> targetSphereService.getStateByKey(key, targetFromState.type)
        Q.all missingStatesP
        .then (missingStates) ->
          (_.reduce missingStates.concat(targetToState), ((acc, state) -> {curr: state, ts: acc.ts.concat({from: acc.curr, to: state})}), {curr: targetFromState, ts: []}).ts
      else
        Q([{from: targetFromState, to: targetToState}])


    ref = (state) ->
      {typeId: "state", id: state.id}

    doTransition = (order, ts) ->
      if _.isEmpty(ts)
        Q(order)
      else
        transition = _.first ts
        targetSphereService.transitionLineItemState order, targetLineItemId, quantity, ref(transition.from), ref(transition.to), date
        .then (resOrder) ->
          doTransition resOrder, _.tail(ts)

    transitions.then (ts) ->
      doTransition targetOrder, ts
      .then (resOrder) ->
        {order: targetOrder.id, lineItem: targetLineItemId, quantity: quantity, transitions: _.map(ts, (t) -> t.from.key + " -> " + t.to.key)}

  lineItemStateSynchronizer = (sourceInfo, msg) ->
    if not supportedMessage(msg)
      Q({processed: true, processingResult: "Not interested"})
    else
      masterSyncInfosP = _.map msg.resource.obj.syncInfo, (si) ->
        sourceInfo.sphere.getChannelByRef(si.channel)
        .then (ch) ->
          {channel: ch, syncInfo: si}

      fromStateP = sourceInfo.sphere.getStateByRef msg.fromState
      toStateP = sourceInfo.sphere.getStateByRef msg.toState
      lineItemIdx = _.find(_.map(msg.resource.obj.lineItems, (li, idx) -> [idx, li]), (box) -> box[1].id is msg.lineItemId)[0]

      Q.spread [Q.all(masterSyncInfosP), fromStateP, toStateP], (masterSyncInfos, fromState, toState) ->
        masterSyncInfo = _.find masterSyncInfos, (i) -> i.channel.key is 'master'

        if not masterSyncInfo?
          throw new Error("Sync Info with master order id is not found for the order: #{msg.resource.id}.")
        else
          tfp = targetSphereService.getStateByKey fromState.key, fromState.type
          ttp = targetSphereService.getStateByKey toState.key, toState.type
          to = targetSphereService.getOrderById masterSyncInfo.syncInfo.externalId

          Q.spread [tfp, ttp, to], (targetFromState, targetToState, targetOrder) ->
            doTargetSateTransition targetOrder, targetOrder.lineItems[lineItemIdx].id, msg.quantity, targetFromState, targetToState, msg.transitionDate
      .then (res) ->
        Q({processed: true, processingResult: res})

  lineItemStateSynchronizer
