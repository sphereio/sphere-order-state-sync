argv = require('optimist')
.usage('Usage: $0')
.argv

Q = require 'q'
{_} = require 'underscore'
{SphereService, MessagePersistenceService, MessageProcessor, Stats} = require '../main'

projects = ["test"]

stats = new Stats {}
messageProcessor = new MessageProcessor stats,
  messageSources:
    _.map projects, (prj) ->
      sphereService = new SphereService stats, {}
      new MessagePersistenceService stats, sphereService, {}
  processors: [
    (sourceInfo, msg) -> Q("Done1")
    (sourceInfo, msg) -> Q("Done2")
  ]

stats.startServer(7777)
stats.startPrinter(true)

messageProcessor.run()