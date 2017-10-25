zfs = require('./zfs')
zfs.doBackup().catch (err) ->
  console.log err
