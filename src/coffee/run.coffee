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

targetSphereService = new SphereService stats,
  processorName: argv.processorName
  connector:
    config: targetProject[0]

count = 0
testProcessor = (sourceInfo, msg) ->
  count = count + 1
  console.info _s.pad("" + count, 4), msg.resource.id, msg.sequenceNumber
  Q({processed: true, processingResult: "Done from '#{JSON.stringify sourceInfo}' msg ID #{msg.id}"})

messageProcessor = new MessageProcessor stats,
  messageSources:
    _.map sourceProjects, (project) ->
      sphereService = new SphereService stats,
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
##  kit.scheduleStateTransitions()
#  targetSphereService.getRecentMessages(util.addDateTime(new Date(), -3, 0, 0))
#.then (foo) ->
#  console.info _.size(foo)
#  console.info foo[0]
#.fail (error) ->
#  console.error "Errror during setup"
#  console.error error.stack
#.done()
