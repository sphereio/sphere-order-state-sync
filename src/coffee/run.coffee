argv = require('optimist')
.usage('Usage: $0')
.argv

Q = require 'q'
{_} = require 'underscore'
{SphereService, MessagePersistenceService, MessageProcessor, Stats} = require '../main'

projects = [
  {project_key: 'SECRET', client_id: 'SECRET', client_secret: 'SECRET'}
]

stats = new Stats {}
messageProcessor = new MessageProcessor stats,
  messageSources:
    _.map projects, (prj) ->
      sphereService = new SphereService stats,
        processorName: "orderStateSync"
        connector:
          config: prj
      new MessagePersistenceService stats, sphereService, {}
  processors: [
    (sourceInfo, msg) -> Q({processed: true, processingResult: "Done from '#{JSON.stringify sourceInfo}' msg ID #{msg.id}"})
    (sourceInfo, msg) -> Q({processed: true, processingResult: "Done in another processor from '#{JSON.stringify sourceInfo}' msg ID #{msg.id}"})
  ]

stats.startServer(7777)
stats.startPrinter(true)
messageProcessor.run()