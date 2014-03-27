Q = require 'q'
fs = require 'q-io/fs'
http = require 'q-io/http'

stdFs = require 'fs'
_ = require('underscore')._
_s = require 'underscore.string'

module.exports =
  # load file from local FS or URL and returns a string promise
  loadFile: (fileOrUrl) ->
    if not fileOrUrl? or _s.isBlank(fileOrUrl)
      Q("")
    else if _s.startsWith(fileOrUrl, 'http')
      http.read fileOrUrl
    else
      fs.exists(fileOrUrl).then (exists) ->
        if exists
          fs.read fileOrUrl, 'r'
        else
          Q.reject new Error("File does not exist: #{fileOrUrl}")
