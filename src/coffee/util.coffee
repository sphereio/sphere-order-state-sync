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
      _.map str.split(/,/), (c) =>
        parts = c.split /:/

        if _.size(parts) is not 3
          throw new Error "project credentions format is wrong!"
        else
          {project_key: parts[0], client_id: parts[1], client_secret: parts[2]}