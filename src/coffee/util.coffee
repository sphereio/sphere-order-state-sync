Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'

###
  Module has some utility functions
###
module.exports =
  parseProjectsCredentials: (str) ->
    if not str?
      []
    else
      _.map str.split(/,/), (c) ->
        parts = c.split /:/

        if _.size(parts) is not 3
          throw new Error "project credentions format is wrong!"
        else
          {project_key: parts[0], client_id: parts[1], client_secret: parts[2]}

  parseDate: (str) ->
    new Date(str)

  addDateTime: (date, hours, minutes, seconds) ->
    @addDateMs date, (hours * 60 * 60 * 1000) + (minutes * 60 * 1000) + (seconds * 1000)

  addDateHours: (date, hours) ->
    @addDateMs date, hours * 60 * 60 * 1000

  addDateMinutes: (date, minutes) ->
    @addDateMs date, minutes * 60 * 1000

  addDateSeconds: (date, minutes) ->
    @addDateMs date, minutes * 1000

  addDateMs: (date, millis) ->
    ms = date.getTime()
    new Date(ms + millis)

  formatDate: (date) ->
    date.toISOString()