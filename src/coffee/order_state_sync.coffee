Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
{util, MessageProcessing, SphereService} = require 'sphere-message-processing'
util1 = require "./util"

p = MessageProcessing.builder()
.processorName "orderStateSync"
.optimistUsage '--targetProject [PROJECT_CREDENTIALS]'
.optimistDemand ['targetProject']
.optimistExtras (o) ->
  o.describe('targetProject', 'Sphere.io credentials of the target project. Format: `prj-key:clientId:clientSecret`.')
  .describe('missingTransitionsConfig', 'The location of the configuration file that defines missing transitions.')
  .alias('targetProject', 't')
.messageCriteria 'resource(typeId="order")'
.messageExpand ['fromState', 'toState']
.build()
.run (argv, stats, requestQueue, cc) ->
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

  Q.spread [util1.loadFile(argv.transitionConfig), spherePromise], (transitionConfigText, targetSphereService) ->
    missingTransitions = if _s.isBlank(transitionConfigText) then [] else JSON.parse(transitionConfigText)

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

      transitions.then (ts) ->
        actualTransitions = _.map ts, (transition) ->
          {quantity: quantity, fromState: ref(transition.from), toState: ref(transition.to), date: date}

        targetSphereService.transitionLineItemStates targetOrder, targetLineItemId, actualTransitions
        .then (resOrder) ->
          {order: resOrder.id, version: resOrder.version, lineItem: targetLineItemId, quantity: quantity, transitions: _.map(ts, (t) -> t.from.key + " -> " + t.to.key)}

    lineItemStateSynchronizer = (sourceInfo, msg) ->
      if not supportedMessage(msg)
        Q({processed: true, processingResult: {ignored: true}})
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