import { readdir } from 'fs/promises';
import { runAllMySQL } from './mysql.js';
import { runAllPostgres } from './postgres.js';
import { runAllMongodb } from './mongodb.js';
import { runAllNeo4j } from './neo4j.js';

import FileHandler, { dirQueries } from '../utils/FileHandler.js';
import { node_pairs, ks, radius } from '../utils/params.js';

async function runAll() {
  await runAllMySQL();
  console.log('');
  await runAllPostgres();
  console.log('');
  await runAllMongodb();
  console.log('');
  await runAllNeo4j();
}

async function readDirs() {
  return await readdir('./out');
}

function mapFileHandlers(dirs) {
  return dirs.map((dir) => {
    return new FileHandler(dir);
  });
}

async function getDistanceResults() {
  let dirs = await readDirs();
  let fileHandlers = mapFileHandlers(dirs);
  let stopwatch = {};
  let results = {};

  for (let pair of node_pairs) {
    for (let fileHandler of fileHandlers) {
      // r[fileHandler.database] = {};
      let filename = fileHandler.distanceFileName(pair);
      if (!stopwatch[filename]) stopwatch[filename] = {};
      if (!results[filename]) results[filename] = {};

      let { time, result } = await fileHandler.readIn({
        queryName: dirQueries.distance,
        filename,
      });
      stopwatch[filename][fileHandler.database] = time;
      results[filename][fileHandler.database] = result;
    }
  }

  console.log({ timestemps: stopwatch });
  console.log({ results });
}

// await runAll();
// await runAllMySQL();
await getDistanceResults();
// let dirs = await readDirs();
// console.log(dirs);
