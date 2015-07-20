zfs = require('./zfs')
zfs.doBackup().catch (err) ->
  console.log "error: #{err}"
