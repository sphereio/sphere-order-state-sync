optimist = require 'optimist'
Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'

{SphereService} = require '../lib/sphere_service'
{Stats} = require '../lib/stats'
{MessageProcessor} = require '../lib/message_processor'
{MessagePersistenceService} = require '../lib/message_persistence'

util = require '../lib/util'

class MessageProcessing
  constructor: (@argv, @statsOptions, @processors, @messageCriteria, @messageExpand) ->
    defaultStatsOptions =
      processor: @argv.processorName

    @stats = new Stats _.extend({}, defaultStatsOptions, @statsOptions)
    @sourceProjects = util.parseProjectsCredentials @argv.sourceProjects

  _createMessageProcessor: () ->
    new MessageProcessor @stats,
      messageSources:
        _.map @sourceProjects, (project) =>
          sphereService = new SphereService @stats,
            additionalMessageCriteria: @messageCriteria
            additionalMessageExpand: @messageExpand
            fetchHours: @argv.fetchHours
            processorName: @argv.processorName
            connector:
              config: project
          new MessagePersistenceService @stats, sphereService,
            awaitTimeout: @argv.awaitTimeout
      processors: @processors
      heartbeatInterval: @argv.heartbeatInterval

  run: (fn) ->
    @stats.startServer(@argv.statsPort)

    if fn?
      @processors.push fn(@argv, @stats)

    @messageProcessor = @_createMessageProcessor()
    @messageProcessor.run()

    if @argv.printStats
      @stats.startPrinter(true)

    console.info "Processor '#{@argv.processorName}' started."

  @builder: () ->
    new MessageProcessingBuilder

class MessageProcessingBuilder
  constructor: () ->
    @usage = 'Usage: $0 --sourceProjects [PROJECT_CREDENTIALS]'
    @demand = ['sourceProjects']
    @statsOptions = {}
    @processors = []
    @additionalMessageExpand = []

  optimistUsage: (extraUsage) ->
    @usage =  @usage + " " + extraUsage
    this

  optimistDemand: (extraDemand) ->
    @demand.push extraDemand
    this

  optimistExtras: (fn) ->
    @optimistExtrasFn = fn
    this

  stats: (options) ->
    @statsOptions = options
    this

  processor: (fn) ->
    @processors.push fn
    this

  messageCriteria: (query) ->
    @additionalMessageCriteria = query
    this

  messageExpand: (expand) ->
    @additionalMessageExpand = expand
    this

  build: () ->
    o = optimist
    .usage(@usage)
    .alias('sourceProjects', 's')
    .alias('statsPort', 'p')
    .alias('help', 'h')
    .describe('help', 'Shows usage info and exits.')
    .describe('sourceProjects', 'Sphere.io project credentials. The messages from these projects would be processed. Format: `prj1-key:clientId:clientSecret[,prj2-key:clientId:clientSecret][,...]`.')
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
    .demand(@demand)

    if @optimistExtrasFn?
      @optimistExtrasFn o

    argv = o.argv

    if (argv.help)
      o.showHelp()
      process.exit 0

    new MessageProcessing argv, @statsOptions, @processors, @additionalMessageCriteria, @additionalMessageExpand

exports.MessageProcessing = MessageProcessing
exports.MessageProcessingBuilder = MessageProcessingBuilder