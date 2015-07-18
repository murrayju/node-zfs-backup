q = require('q')
child_process = require('child_process')
exec = child_process.exec
spawn = child_process.spawn
config = require('./config.json')

deferIt = (fn) ->
  defer = q.defer()
  fn(defer)
  return defer.promise

getDatasets = () ->
  deferIt (defer) ->
    exec "zfs get -s local -H -o name,value #{config.prop}", (err, stdout, stderr) ->
      return defer.reject(err) if err?

      lines = stdout.trim().split(/\n/)
      datasets = {}
      for line in lines
        tokens = line.split(/\s/)
        datasets[tokens[0]] = tokens[1]
      defer.resolve(datasets)

getLocalSnapshots = (dataset) ->
  defer = q.defer()
  exec "zfs list -t snapshot -H -S creation -o name -d 1 #{dataset}", (err, stdout, stderr) ->
    return defer.reject(err) if err?

    return defer.resolve(stdout.split(/\n/))

  return defer.promise

getDatasets()
  .then (datasets) ->
    for ds,type of datasets
      do (ds) ->
        getLocalSnapshots(ds)
          .then (snaps) ->
            console.log("dataset: #{ds}")
            console.log(snap) for snap in snaps
          .catch (err) ->
            console.log "error: #{err}"
  .catch (err) ->
    console.log "error: #{err}"

