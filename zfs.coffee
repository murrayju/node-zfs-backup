q = require('q')
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
  deferCmd(cmd).spread (stdout, stderr) ->
    return stdout.split(/\n/)

getSnapshotTimestamp = (snap) ->
  deferCmd("zfs get -Hp -o value creation #{snap}").spread (stdout, stderr) ->
    return parseInt(stdout, 10)

parseSnapshot = (snap) ->
  parts = snap.match(/^([^@]+@)(.+)$/)
  return {
    dataset: parts[1]
    name: parts[2]
  }

findFirstCommon = (setA, setB) ->
  for a in setA
    for b in setB
      return a if a == b
  return null

# Daily, or whatever pref says
findFirstDaily = (snaps) ->
  tag = if config?.tag? then config.tag else 'daily'
  regex = new RegExp(tag, 'i')
  for snap in snaps
    return snap if regex.test(snap)

doBackup = (ds, type, baseSnap, targetSnap) ->
  console.log("dataset: #{ds}, base: #{baseSnap}, new: #{targetSnap}")
  if baseSnap == targetSnap
    console.log "Remote server already up to date"
    return
  return q.all([
    getSnapshotTimestamp(baseSnap)
    getSnapshotTimestamp(targetSnap)
  ])
    .spread (baseTS, targetTS) ->
      if targetTS < baseTS
        return q.reject("Target snapshot #{targetSnap} is older than base snapshot #{baseSnap}!")
      parsedBase = parseSnapshot(baseSnap)
      remPool = config?.remote?.pool || 'tank'
      cmd = "zfs send -v -I #{parsedBase.name} -R #{targetSnap} | #{sshCmd} zfs recv -dvF #{remPool}"
      console.log cmd
      return deferCmd(cmd).spread (stdout, stderr) ->
        console.log stdout


# just test code
getDatasets()
  .then (datasets) ->
    for ds,type of datasets
      do (ds) ->
        q.all([
          getSnapshots(ds, false)
          getSnapshots(ds, true)
        ])
          .spread (localSnaps, remoteSnaps) ->
            baseSnap = findFirstCommon(remoteSnaps, localSnaps)
            targetSnap = findFirstDaily(localSnaps)
            return doBackup(ds, type, baseSnap, targetSnap)
          .catch (err) ->
            console.log "error: #{err}"
  .catch (err) ->
    console.log "error: #{err}"

