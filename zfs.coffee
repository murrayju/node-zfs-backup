q = require('q')
_ = require('lodash')
child_process = require('child_process')
exec = child_process.exec
spawn = child_process.spawn
config = require('./config.json')

sshUser = config?.remote?.user || ''
sshCmd = "ssh "
if config?.remote?.user?
  sshCmd += config.remote.user + '@'
sshCmd += (config?.remote?.host || 'localhost')
if config?.remote?.port?
  sshCmd += " -p #{config.remote.port}"

deferIt = (fn) ->
  defer = q.defer()
  fn(defer)
  return defer.promise

deferCmd = (cmd) ->
  deferIt (defer) ->
    exec cmd, (err, stdout, stderr) ->
      return defer.reject(err) if err?
      return defer.resolve([stdout, stderr])

getDatasets = () ->
  deferCmd("zfs get -s local -H -o name,value #{config.prop}").spread (stdout, stderr) ->
    lines = stdout.trim().split(/\n/)
    datasets = {}
    for line in lines
      tokens = line.split(/\s/)
      datasets[tokens[0]] = tokens[1]
    return datasets

getSnapshots = (dataset, remote) ->
  cmd = if remote then sshCmd + ' ' else ''
  cmd += "zfs list -t snapshot -H -S creation -o name -d 1 #{dataset}"
  deferCmd(cmd)
  .spread (stdout, stderr) ->
    return _.compact(stdout.split(/\n/))
  .catch -> return null

getSnapshotTimestamp = (snap) ->
  deferCmd("zfs get -Hp -o value creation #{snap}").spread (stdout, stderr) ->
    return parseInt(stdout, 10)

parseSnapshot = (snap) ->
  parts = snap.match(/^([^@]+@)(.+)$/)
  return {
    dataset: parts?[1]
    name: parts?[2]
  }

destroySnapshot = (snap, dryRun=true) ->
  cmd = "zfs destroy #{snap}"
  console.log(cmd)
  return q.when(false) if dryRun
  deferCmd(cmd)

findFirstCommon = (setA, setB) ->
  return null unless setA?.length && setB?.length
  for a in setA
    for b in setB
      return a if a == b
  return null

# Daily, or whatever pref says
findFirstDaily = (snaps) ->
  return null unless snaps?.length
  tag = if config?.tag? then config.tag else 'daily'
  regex = new RegExp(tag, 'i')
  for snap in snaps
    return snap if regex.test(snap)
  return null

doSendRecv = (ds, type, baseSnap, targetSnap) ->
  console.log("dataset: #{ds}, base: #{baseSnap}, new: #{targetSnap}")
  if baseSnap == targetSnap
    console.log "Remote server already up to date"
    return
  remPool = config?.remote?.pool || 'tank'
  unless baseSnap?
    # No base for incremental send, do initial full send
    cmd = "zfs send -v -R #{targetSnap} | #{sshCmd} zfs recv -dvF #{remPool}"
    console.log cmd
    return deferCmd(cmd).spread (stdout, stderr) ->
      console.log stdout

  return q.all([
    getSnapshotTimestamp(baseSnap)
    getSnapshotTimestamp(targetSnap)
  ])
    .spread (baseTS, targetTS) ->
      if targetTS < baseTS
        return q.reject("Target snapshot #{targetSnap} is older than base snapshot #{baseSnap}!")
      parsedBase = parseSnapshot(baseSnap)
      cmd = "zfs send -v -I #{parsedBase.name} -R #{targetSnap} | #{sshCmd} zfs recv -dvF #{remPool}"
      console.log cmd
      return deferCmd(cmd).spread (stdout, stderr) ->
        console.log stdout

foreachDataset = (cbFn) ->
  getDatasets().then (datasets) ->
    _.chain(datasets)
    .map (type, ds) -> {type, ds}
    .filter (d) -> d.type == 'fullpath'
    .sortBy (d) -> -d.ds.length # deeper paths first
    .reduce(
      (promise, d) -> promise.then ->
        q.all([
          getSnapshots(d.ds, false)
          getSnapshots(d.ds, true)
        ]).spread (localSnaps, remoteSnaps) ->
          q.when(cbFn?(d.ds, d.type, localSnaps, remoteSnaps))
      q.when(true)
    )
    .value()

doBackup = ->
  return foreachDataset (ds, type, localSnaps, remoteSnaps) ->
    targetSnap = findFirstDaily(localSnaps)
    baseSnap = findFirstCommon(remoteSnaps, localSnaps)
    unless baseSnap?
      # No common snapshot, use the latest available remote
      baseSnap = _.head(remoteSnaps)
    return doSendRecv(ds, type, baseSnap, targetSnap)

purgeOldSnaps = (dryRun=true) ->
  foreachDataset (ds, type, localSnaps, remoteSnaps) ->
    baseSnap = findFirstCommon(remoteSnaps, localSnaps)
    unless baseSnap
      console.log "No common base for #{ds}"
      return
    getSnapshotTimestamp(baseSnap).then (baseTS) ->
      q.all _.map localSnaps, (snap) ->
        getSnapshotTimestamp(snap).then (snapTS) ->
          if snapTS < baseTS
            destroySnapshot(snap, dryRun)

# Export public fns
module.exports =
  doBackup: doBackup
  purgeOldSnaps: purgeOldSnaps

