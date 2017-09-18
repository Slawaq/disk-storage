const fs = require('fs')
const path = require('path')
const EOL = require('os').EOL
const Promise = require('bluebird')

const readFile = Promise.promisify(fs.readFile)
const writeFile = Promise.promisify(fs.writeFile)
const read = Promise.promisify(fs.read)
const open = Promise.promisify(fs.open)
const close = Promise.promisify(fs.close)
const mkdirp = Promise.promisify(require('mkdirp'))
const appendFile = Promise.promisify(fs.appendFile)
const isNotAccessible = Promise.promisify((path, cb) => fs.access(path, fs.constants.R_OK | fs.constants.W_OK, (err) => cb(null, err)))

const defaultDataFolder = path.join(__dirname, 'data')
const dataFolder = process.env.DATA_FOLDER || (() => { console.log(`Warning! Env variable 'DATA_FOLDER' is not defined, ${defaultDataFolder} is using as data folder.`); return defaultDataFolder })()
const cursorFilename = 'cursor'
const cursorPath = path.join(dataFolder, cursorFilename)
const fromDate = Date.UTC(2016, 0)
const hourInMs = 60 * 60 * 1000
const readBufferSize = 128 * 1024
const readParallelism = 4
const lineBatchSize = 1000
const logAboutMissingDataLog = false

const getDataFolder = date => path.join(...[
  dataFolder,
  date.getUTCFullYear().toString(),
  (date.getUTCMonth() + 1).toString().padStart(2, '0'),
  date.getUTCDate().toString().padStart(2, '0')
])

const getDataFilePath = date => path.join(getDataFolder(date), date.getUTCHours().toString().padStart(2, '0'))

const parseCursor = text => {
  let [ timestampText, positionText ] = text.split(':')

  let timestamp = parseInt(timestampText, 10)
  let position = parseInt(positionText, 10)

  if (isNaN(timestamp) || isNaN(position)) {
    console.log(`Warning! Cursor entry is not valid, timestamp: ${timestampText}, position: ${positionText}, will be used default options, from: ${fromDate}.`)
    return { timestamp: +fromDate, position: 0 }
  } else
    return { timestamp, position }
}

const saveCursor = async ({ timestamp, position }) => {
  let text = `${timestamp}:${position}`
  await writeFile(cursorPath, text)
}

const getOrCreateCursor = async () => {
  let cursorNotAccessible = await isNotAccessible(cursorPath)

  if (cursorNotAccessible && cursorNotAccessible.code === 'ENOENT') {
    console.log(`Warning! Cursor file ${cursorPath} is not exist, it will be created.`)
    await close(await open(cursorPath, 'w'))
  } else if (cursorNotAccessible) {
    console.log(`Fatal! Cursor file error ${cursorPath}${EOL}.`, cursorNotAccessible)
    return cursorNotAccessible
  }

  let cursor = parseCursor((await readFile(cursorPath)).toString('utf8'))
  await saveCursor(cursor)

  return cursor
}

const getDataFiles = async () => {
  let cursor = await getOrCreateCursor()

  let from = cursor.timestamp
  let to = Date.now()
  let interval = (to - from)

  if (interval < 0)
    throw new Error(`From is bigger then to, from: ${from}, to: ${to}, check cursor file!`)

  let intervalInHours = Math.ceil(interval / hourInMs)

  let dataFiles = new Array(intervalInHours).fill(null)
    .map((_, hour) => ({
      position: 0,
      path: getDataFilePath(new Date(from + hour * hourInMs)),
      timestamp: from + hour * hourInMs
    }))
  dataFiles[0].position = cursor.position

  return dataFiles
}

const readFilePart = f => async (data, position = 0) => {
  data.readBytes = await read(f, data.buffer, 0, readBufferSize, position)
}

const processFile = (file, position = 0) => {
  let readPart = readFilePart(file)
  let data = new Array(readParallelism).fill(null)
    .map(() => ({
      readBytes: null,
      buffer: Buffer.allocUnsafe(readBufferSize)
    }))
  let parallel = new Array(readParallelism).fill(null)
    .map((_, i) => {
      let from = position + readBufferSize * i
      let x = data[i]
      return offset => readPart(x, from + offset)
    })

  let i = 0
  let readBytes = position
  let hasNextBytes = true
  const step = readBufferSize * readParallelism

  return async (handler) => {
    while (hasNextBytes) {
      await Promise.all(parallel.map(x => x(i * step)))

      hasNextBytes = data[data.length - 1].readBytes === readBufferSize
      readBytes += hasNextBytes ? step : data.reduce((r, x) => r + x.readBytes, 0)

      let chunk = data.map(x => x.buffer.toString('utf8', 0, Math.max(0, x.readBytes))).join('')

      await handler(chunk, readBytes)

      i++
    }

    return readBytes
  }
}

let readFileLines = (file, position = 0) => {
  let processChunk = processFile(file, position)
  let lastLine = ''

  return async (handler) => {
    let readBytes = await processChunk(async (chunk, readBytes) => {
      let lines = chunk.split(EOL)

      lines[0] = lastLine + lines[0]

      lastLine = lines[lines.length - 1]
      lines.pop()

      let sentBytes = 0
      const bytesFrom = readBytes - chunk.length

      for (let line of lines) {
        if (line.length === 0) continue
        sentBytes += line.length
        await handler(line, bytesFrom + sentBytes)
      }
    })

    if (lastLine.length > 0)
      await handler(lastLine, readBytes)

    return readBytes
  }
}

const readFileBatchLines = (file, position = 0) => {
  let readLines = readFileLines(file, position)
  let lines = []

  return async (handler) => {
    let readBytes = await readLines(async (line, readBytes) => {
      lines.push(line)
      if (lines.length === lineBatchSize) {
        await handler(lines, readBytes)
        lines = []
      }
    })

    if (lines.length > 0)
      await handler(lines, readBytes)

    return readBytes
  }
}

const prepareData = data => {
  let text = ''
  for (let x of data) {
    for (let [ key, value ] of Object.entries(x))
      text += key.replace(/\n/g, ' ') + ':' + value.toString().replace(/[\n|]/g, ' ') + '|'
    text += EOL
  }
  return text
}

const parseData = text => {
  let data = {}

  for (let splitted of text.split('|')) {
    let [ key, value ] = splitted.split(':')

    if (key.trim().length > 0)
      data[key] = value
  }

  return data
}

const notifySubscibers = subscribers => async data => {
  for (let subscriber of subscribers) {
    try {
      await subscriber(data)
    } catch (e) {
      console.log(`Warning! Subscriber failed to process data.`)
    }
  }
}

const createPubSub = () => {
  let subscribers = []
  return {
    send: notifySubscibers(subscribers),
    on: subscriber => subscribers.push(subscriber)
  }
}

const readNewData = pubSub => async () => {
  for (let { position, path, timestamp } of await getDataFiles()) {
    try {
      let file = await open(path, 'r')
      try {
        let readLines = readFileBatchLines(file, position)
        let processBatch = async (lines, readBytes) => {
          await pubSub.send(lines.map(parseData))
          await saveCursor({ position: readBytes, timestamp })
        }
        await readLines(processBatch)
      } catch (e) {
        console.log(`Warning! Failed to read data ${path}:${position}${EOL}${e.stack}.`)
      } finally {
        await close(file)
      }
    } catch (e) {
      if (logAboutMissingDataLog)
        console.log(`Warning! File ${path} is not accessible for reading.`)
    }
  }
}

module.exports = {
  reader: () => {
    let pubSub = createPubSub()

    setInterval(readNewData(pubSub), 5000)

    return pubSub
  },
  /**
   * @param {Object[]} data
   */
  writer: async (data) => {
    if (data.length === 0) return
    let now = new Date()
    let dataPath = getDataFolder(now)

    await mkdirp(dataPath)

    await appendFile(getDataFilePath(now), prepareData(data))
  }
}
