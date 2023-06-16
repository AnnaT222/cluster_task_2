import { fork } from "child_process";
import cluster from "cluster";
import csv from "csv-parser";
import fs, { createReadStream } from 'fs';
import os, { availableParallelism } from "os"
import path from "path";

if (cluster.isPrimary) {

  const data = process.argv[2]
  let csvs = fs.readdirSync(data).filter((e) => path.extname(e) === ".csv")

  if (!csvs.length) {
    throw "There isn't any csv files"
  }

  let minCores = Math.min(csvs.length, os.availableParallelism())

  for (let i = 0; i < minCores; i++) {
    cluster.fork();
  }

  let tasks = distribution(csvs, minCores)

  cluster.on('message', (worker, msg) => {
    if (msg.ready) {
      worker.send(tasks[worker.id - 1])
    }
  })


} else {
  process.send({ ready: true })

  let allProm = []

  process.on('message', (collection) => {
    for (let i = 0; i < collection.length; i++) {
      allProm.push(parseFiles(collection[i]))
    }
    Promise.all(allProm).then(() => {
      process.kill(process.pid)
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
        if (!fs.existsSync(path.join(process.argv[2], "converted"))) {
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
    files[j++].push(allFiles[i]);
    if (j == cores) j = 0
  }

  return files

}