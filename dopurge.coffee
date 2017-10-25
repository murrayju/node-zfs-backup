zfs = require('./zfs')
dryRun = true
process.argv.forEach (arg) ->
  if arg == '--purge'
    dryRun = false

if dryRun
  console.log("Dry run. Pass --purge to do it for real.")
zfs.purgeOldSnaps(dryRun).catch (err) ->
  console.log "error: #{err}"
