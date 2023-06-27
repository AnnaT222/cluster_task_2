import csv from "csv-parser";
import fs from 'fs';
import path from "path";
import { isMainThread, parentPort, Worker } from "worker_threads";

if (isMainThread) {

  const data = process.argv[2]
  let csvs = fs.readdirSync(data).filter((e) => path.extname(e) === ".csv")

  if (!csvs.length) {
    throw "There isn't any csv files"
  }

  let minCores = Math.min(csvs.length, 10)
  let tasks = distribution(csvs, minCores)

  for (let i = 0; i < minCores; i++) {
    let worker = new Worker("./")
    worker.on('online', () => {
      worker.postMessage(tasks[i])

    })
  }
}
else {
  let allProm = []

  parentPort.on('message', (collection) => {
    for (let i = 0; i < collection.length; i++) {
      allProm.push(parseFiles(collection[i]))
    }

    Promise.all(allProm).then(() => {
      parentPort.close()
    })
  })

}


function parseFiles(file) {

  let parsedData = []

  return new Promise((res, rej) => {

    fs.createReadStream(file)
      .pipe(csv())
      .on('data', (data) => {
        parsedData.push(data)
      })
      .on('end', () => {
        if (!fs.existsSync(path.join("./", "converted"))) {
          fs.mkdirSync("converted")
        }


        const fileName = `${path.basename(file)}.json`

        const writableFile = fs.createWriteStream(path.resolve("converted", fileName))
        writableFile.write(JSON.stringify(parsedData));
        writableFile.on("error", (err) => rej(err))
        writableFile.on("finish", () => res("Yes"))
        writableFile.end()

      })
  })

}

function distribution(allFiles, cores) {
  const files = []

  for (let i = 0; i < cores; i++) {
    files.push([])
  }


  for (let i = 0, j = 0; i < allFiles.length; i++) {
    files[j++].push(path.resolve(process.argv[2], allFiles[i]));
    if (j == cores) j = 0
  }

  return files

}