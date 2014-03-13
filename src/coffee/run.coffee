Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
{SphereService, MessagePersistenceService, MessageProcessor, Stats} = require '../main'
util = require '../lib/util'
testKit = require '../lib/sphere_test_kit'

optimist = require('optimist')
.usage('Usage: $0 --sourceProjects [PROJECT_CREDENTIALS] --targetProject [PROJECT_CREDENTIALS]')
.alias('sourceProjects', 's')
.alias('targetProject', 't')
.alias('statsPort', 'p')
.describe('sourceProjects', 'Sphere.io project credentials. The messages from these projects would be processed. Format: `prj1-key:clientId:clientSecret[,prj2-key:clientId:clientSecret][,...]`.')
.describe('targetProject', 'Sphere.io credentials of the target project. Format: `prj-key:clientId:clientSecret`.')
.describe('statsPort', 'The port of the stats HTTP server.')
.describe('processorName', 'The name of this processor. Name is used to rebember, which messaged are already processed.')
.describe('printStats', 'Whether to print stats to the console every 3 seconds.')
.describe('awaitTimeout', 'How long to wait for the message ordering (in ms).')
.describe('heartbeatInterval', 'How often are messages retrieved from sphere project (in ms).')
.describe('fetchHours', 'How many hours of messages should be fetched (in hours).')
.default('statsPort', 7777)
.default('awaitTimeout', 120000)
.default('heartbeatInterval', 2000)
.default('fetchHours', 24)
.default('processorName', "orderStateSync")
.demand(['sourceProjects', 'targetProject'])

argv = optimist.argv

if (argv.help)
  optimist.showHelp()
  process.exit 0

stats = new Stats
  processor: argv.processorName

sourceProjects = util.parseProjectsCredentials argv.sourceProjects
targetProject = util.parseProjectsCredentials argv.sourceProjects

if _.size(targetProject) > 1
  throw new Error("There and be only one target project.")

missingTransitions = [
  {from: 'B', to: 'D', missing: ['C']}
]

targetSphereService = new SphereService stats,
  processorName: argv.processorName
  connector:
    config: targetProject[0]

supportedMessage = (msg) ->
  msg.resource.typeId is 'order' and msg.type is 'LineItemStateTransition'

doTargetSateTransition = (targetOrder, targetLineItemId, quantity, targetFromState, targetToState) ->
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
      targetSphereService.transitionLineItemState order, targetLineItemId, quantity, ref(transition.from), ref(transition.to)
      .then (resOrder) ->
        doTransition resOrder, _.tail(ts)

  transitions.then (ts) ->
    doTransition targetOrder, ts
    .then (resOrder) ->
      {order: targetOrder.id, lineItem: targetLineItemId, quantity: quantity, transitions: _.map(ts, (t) -> t.from.key + " -> " + t.to.key)}

testProcessor = (sourceInfo, msg) ->
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
          doTargetSateTransition targetOrder, targetOrder.lineItems[lineItemIdx].id, msg.quantity, targetFromState, targetToState
    .then (res) ->
      Q({processed: true, processingResult: res})

messageProcessor = new MessageProcessor stats,
  messageSources:
    _.map sourceProjects, (project) ->
      sphereService = new SphereService stats,
        additionalMessageCriteria: 'resource(typeId="order")'
        additionalMessageExpand: ['fromState', 'toState']
        fetchHours: argv.fetchHours
        processorName: argv.processorName
        connector:
          config: project
      new MessagePersistenceService stats, sphereService,
        awaitTimeout: argv.awaitTimeout
  processors: [testProcessor]
  heartbeatInterval: argv.heartbeatInterval

stats.startServer(argv.statsPort)
messageProcessor.run()

if argv.printStats
  stats.startPrinter(true)

console.info "Processor '#{argv.processorName}' started."

#sphereTestKit = new testKit.SphereTestKit targetSphereService
#sphereTestKit.setupProject()
#.then (kit) ->
#  console.info "Done"
#
#  kit.scheduleStateTransitions()
##  targetSphereService.getRecentMessages(util.addDateTime(new Date(), -3, 0, 0))
#.then (foo) ->
#  console.info _.size(foo)
#  console.info foo[0]
#.fail (error) ->
#  console.error "Errror during setup"
#  console.error error.stack
#.done()
