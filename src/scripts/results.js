import { readdir } from 'fs/promises';
import { runAllMySQL } from './mysql.js';
import { runAllPostgres } from './postgres.js';
import { runAllMongodb } from './mongodb.js';
import { runAllNeo4j } from './neo4j.js';

import FileHandler, { dirQueries } from '../utils/FileHandler.js';
import { node_pairs, ks, radius } from '../utils/params.js';
import { exit } from 'process';

export async function runAll() {
  await runAllMySQL();
  console.log('');
  await runAllPostgres();
  console.log('');
  await runAllMongodb();
  console.log('');
  await runAllNeo4j();
}

// export async function readDirs() {
//   return await readdir('./out');
// }

// function mapFileHandlers(dirs) {
//   return dirs.map((dir) => {
//     return new FileHandler(dir);
//   });
// }

export async function availbleFileHandlers() {
  let dirs = await readdir('./out');
  if (dirs.length === 0) throw new Error('No files found in ./out');

  return dirs.map((dir) => {
    return new FileHandler(dir);
  });
}

export async function getDistanceResults() {
  let fileHandlers = await availbleFileHandlers();
  let timestemps = {};
  let results = {};

  for (let pair of node_pairs) {
    for (let fileHandler of fileHandlers) {
      // r[fileHandler.database] = {};
      let filename = fileHandler.distanceFileName(pair);
      if (!timestemps[filename]) timestemps[filename] = {};
      if (!results[filename]) results[filename] = {};

      let { time, result } = await fileHandler.readIn({
        queryName: dirQueries.distance,
        filename,
      });
      timestemps[filename][fileHandler.database] = time;
      results[filename][fileHandler.database] = result;
    }
  }

  // console.log({ timestemps: timestemps });
  // console.log({ results });
  return { timestemps, results };
}

export async function getRadiusResults() {
  let fileHandlers = await availbleFileHandlers();
  let timestemps = {};
  let results = {};

  for (let pair of node_pairs) {
    for (let r of radius) {
      for (let fileHandler of fileHandlers) {
        let filename = fileHandler.radiusRQFileName({ ...pair, radius: r });
        if (!timestemps[filename]) timestemps[filename] = {};
        if (!results[filename]) results[filename] = {};

        let { time, result } = await fileHandler.readIn({
          queryName: dirQueries.radius,
          filename,
        });
        timestemps[filename][fileHandler.database] = time;
        results[filename][fileHandler.database] = result;
      }
    }
  }

  console.log({ timestemps });
  // console.log({ results });
  return { timestemps, results };
}

await getRadiusResults();

export async function getWindowResults() {
  let fileHandlers = await availbleFileHandlers();
  let timestemps = {};
  let results = {};

  for (let pair of node_pairs) {
    for (let fileHandler of fileHandlers) {
      let filename = fileHandler.windowRQFileName(pair);
      if (!timestemps[filename]) timestemps[filename] = {};
      if (!results[filename]) results[filename] = {};

      let { time, result } = await fileHandler.readIn({
        queryName: dirQueries.window,
        filename,
      });
      timestemps[filename][fileHandler.database] = time;
      results[filename][fileHandler.database] = result;
    }
  }

  // console.log({ timestemps });
  // console.log({ results });
  // console.log(results['windowRQ_7410560799_4662482749.json']['mongodb']);
  return { timestemps, results };
}

export async function getRangeCountResults() {
  let fileHandlers = await availbleFileHandlers();

  let timestempsWindow = {};
  let resultsWindow = {};
  let timestempsRadius = {};
  let resultsRadius = {};

  for (let pair of node_pairs) {
    for (let fileHandler of fileHandlers) {
      let filename = fileHandler.rangeCountFileName({
        ...pair,
        type: dirQueries.windowCount,
      });
      if (!timestempsWindow[filename]) timestempsWindow[filename] = {};
      if (!resultsWindow[filename]) resultsWindow[filename] = {};

      let { time, result } = await fileHandler.readIn({
        queryName: dirQueries.windowCount,
        filename,
      });

      timestempsWindow[filename][fileHandler.database] = time;
      resultsWindow[filename][fileHandler.database] = result;

      for (let r of radius) {
        let filename = fileHandler.rangeCountFileName({
          ...pair,
          radius: r,
          type: dirQueries.radiusCount,
        });
        if (!timestempsRadius[filename]) timestempsRadius[filename] = {};
        if (!resultsRadius[filename]) resultsRadius[filename] = {};

        let { time, result } = await fileHandler.readIn({
          queryName: dirQueries.radiusCount,
          filename,
        });

        timestempsRadius[filename][fileHandler.database] = time;
        resultsRadius[filename][fileHandler.database] = result;

        // break;
      }
      // break;
    }
    // break;
  }

  // console.log({ timestempsWindow });
  // console.log({ resultsWindow });
  // console.log({ timestempsRadius });
  // console.log({ resultsRadius });
  return { timestempsWindow, resultsWindow, timestempsRadius, resultsRadius };
}

export async function getKNNResults() {
  let fileHandlers = await availbleFileHandlers();
  let timestemps = {};
  let results = {};

  for (let pair of node_pairs) {
    for (let k of ks) {
      for (let fileHandler of fileHandlers) {
        let filename = fileHandler.knnFileName({ ...pair, k });
        if (!timestemps[filename]) timestemps[filename] = {};
        if (!results[filename]) results[filename] = {};

        let { time, result } = await fileHandler.readIn({
          queryName: dirQueries.knn,
          filename,
        });

        timestemps[filename][fileHandler.database] = time;
        results[filename][fileHandler.database] = result;
      }
    }
  }

  // console.log({ timestemps });
  // console.log({ results });
  return { timestemps, results };
}

export async function getKClosestPairs() {
  let fileHandlers = await availbleFileHandlers();
  let timestemps = {};
  let results = {};

  // console.log(fileHandlers);

  for (let k of ks) {
    for (let fileHandler of fileHandlers) {
      // let filename = fileHandler.kClosestPairsFileName({ k });
      let filename = fileHandler.kClosestPairFileName({ k });
      if (!timestemps[filename]) timestemps[filename] = {};
      if (!results[filename]) results[filename] = {};

      let { time, result } = await fileHandler.readIn({
        queryName: dirQueries.kClosestPair,
        filename,
      });

      timestemps[filename][fileHandler.database] = time;
      results[filename][fileHandler.database] = result;
    }
  }

  // exit(0);

  // console.log({ timestemps });
  // console.log({ results });
  return { timestemps, results };
}

// await runAll();
// await runAllMySQL();
// await runAllPostgres();
// await runAllMongodb();
// await runAllNeo4j();
// await getDistanceResults();
// await getRadiusResults();
// await getWindowResults();
// await getRangeCountResults();
// await getKNNResults();
// await getKClosestPairs();
