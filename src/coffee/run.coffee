Q = require 'q'
{_} = require 'underscore'
{SphereService, MessagePersistenceService, MessageProcessor, Stats} = require '../main'
util = require '../lib/util'

optimist = require('optimist')
.usage('Usage: $0 --sourceProjects [PROJECT_CREDENTIALS] --targetProject [PROJECT_CREDENTIALS]')
.alias('sourceProjects', 's')
.alias('targetProject', 't')
.alias('clientSecret', 's')
.alias('statsPort', 'p')
.describe('sourceProjects', 'Sphere.io project credentials. The messages from these projects would be processed. It has following format: `prj1-key:clientId:clientSecret[,prj2-key:clientId:clientSecret][,...]`.')
.describe('targetProject', 'Sphere.io credentials of the target project. It has following format: `prj-key:clientId:clientSecret`.')
.describe('statsPort', 'The port of the stats HTTP server.')
.describe('processorName', 'The name of this processor. It would be used to rebember, which messaged are already processed.')
.describe('printStats', 'Whether to print stats to the consome every 3 seconds.')
.describe('awaitTimeout', 'How long to wait for the message ordering (in ms).')
.describe('heartbeatInterval', 'How How often are messages retrieved from sphere project (in ms).')
.default('statsPort', 7777)
.default('awaitTimeout', 10000)
.default('heartbeatInterval', 500)
.default('processorName', "orderStateSync")
.demand(['sourceProjects', 'targetProject'])

argv = optimist.argv

if (argv.help)
  optimist.showHelp()
  process.exit 0

stats = new Stats {}

sourceProjects = util.parseProjectsCredentials argv.sourceProjects
targetProject = util.parseProjectsCredentials argv.sourceProjects

if _.size(targetProject) > 1
  throw new Error("There and be only one target project.")

targetSphereService = new SphereService stats,
  processorName: argv.processorName
  connector:
    config: targetProject[0]

processors = [
  (sourceInfo, msg) -> Q({processed: true, processingResult: "Done from '#{JSON.stringify sourceInfo}' msg ID #{msg.id}"})
  (sourceInfo, msg) -> Q({processed: true, processingResult: "Done in another processor from '#{JSON.stringify sourceInfo}' msg ID #{msg.id}"})
]
messageProcessor = new MessageProcessor stats,
  messageSources:
    _.map sourceProjects, (project) ->
      sphereService = new SphereService stats,
        processorName: argv.processorName
        connector:
          config: project
      new MessagePersistenceService stats, sphereService,
        awaitTimeout: argv.awaitTimeout
  processors: processors
  heartbeatInterval: argv.heartbeatInterval

stats.startServer(argv.statsPort)
messageProcessor.run()

if argv.printStats
  stats.startPrinter(true)