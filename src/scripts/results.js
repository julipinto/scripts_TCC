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
      let keyfile = filename.split('.')[0];
      if (!timestemps[keyfile]) timestemps[keyfile] = {};
      if (!results[keyfile]) results[keyfile] = {};

      let { time, result } = await fileHandler.readIn({
        queryName: dirQueries.distance,
        filename,
      });
      timestemps[keyfile][fileHandler.database] = time;
      results[keyfile][fileHandler.database] = result;
    }
  }

  // console.log({ timestemps });
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
        let keyfile = filename.split('.')[0];
        let [_, pid, fr] = keyfile.split('_');

        if (!timestemps[pid]) timestemps[pid] = {};
        if (!timestemps[pid][fr]) timestemps[pid][fr] = {};
        if (!results[pid]) results[pid] = {};
        if (!results[pid][fr]) results[pid][fr] = {};

        let { time, result } = await fileHandler.readIn({
          queryName: dirQueries.radius,
          filename,
        });

        timestemps[pid][fr][fileHandler.database] = time;
        results[pid][fr][fileHandler.database] = result;
      }
    }
  }
  // console.log({ timestemps });
  // console.log({ results });

  // console.log(Object.keys(timestemps).length);
  return { timestemps, results };
}

// await getRadiusResults();

export async function getWindowResults() {
  let fileHandlers = await availbleFileHandlers();
  let timestemps = {};
  let results = {};

  for (let pair of node_pairs) {
    for (let fileHandler of fileHandlers) {
      let filename = fileHandler.windowRQFileName(pair);
      let keyfile = filename.split('.')[0];
      if (!timestemps[keyfile]) timestemps[keyfile] = {};
      if (!results[keyfile]) results[keyfile] = {};

      let { time, result } = await fileHandler.readIn({
        queryName: dirQueries.window,
        filename,
      });

      timestemps[keyfile][fileHandler.database] = time;
      results[keyfile][fileHandler.database] = result;
    }
  }

  // console.log({ timestemps });
  // console.log(results['windowRQ_7410560799_4662482749']);
  return { timestemps, results };
}

// await getWindowResults();

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
      let keyfile = filename.split('.')[0];

      if (!timestempsWindow[keyfile]) timestempsWindow[keyfile] = {};
      if (!resultsWindow[keyfile]) resultsWindow[keyfile] = {};

      let { time, result } = await fileHandler.readIn({
        queryName: dirQueries.windowCount,
        filename,
      });

      timestempsWindow[keyfile][fileHandler.database] = time;
      resultsWindow[keyfile][fileHandler.database] = result;

      for (let r of radius) {
        let filename = fileHandler.rangeCountFileName({
          ...pair,
          radius: r,
          type: dirQueries.radiusCount,
        });

        let keyfile = filename.split('.')[0];
        let [_, pid, fr] = keyfile.split('_');

        if (!timestempsRadius[pid]) timestempsRadius[pid] = {};
        if (!timestempsRadius[pid][fr]) timestempsRadius[pid][fr] = {};
        if (!resultsRadius[pid]) resultsRadius[pid] = {};
        if (!resultsRadius[pid][fr]) resultsRadius[pid][fr] = {};

        let { time, result } = await fileHandler.readIn({
          queryName: dirQueries.radiusCount,
          filename,
        });

        timestempsRadius[pid][fr][fileHandler.database] = time;
        resultsRadius[pid][fr][fileHandler.database] = result;
      }
    }
  }

  // console.log({ timestempsWindow });
  // console.log({ resultsWindow });
  // console.log({ timestempsRadius });
  // console.log({ resultsRadius });
  return { timestempsWindow, resultsWindow, timestempsRadius, resultsRadius };
}

// await getRangeCountResults();

export async function getKNNResults() {
  let fileHandlers = await availbleFileHandlers();
  let timestemps = {};
  let results = {};

  for (let pair of node_pairs) {
    for (let k of ks) {
      for (let fileHandler of fileHandlers) {
        let filename = fileHandler.knnFileName({ ...pair, k });
        let f = filename.split('.')[0];
        let [_, pid, fk] = f.split('_');

        if (!timestemps[pid]) timestemps[pid] = {};
        if (!timestemps[pid][fk]) timestemps[pid][fk] = {};
        if (!results[pid]) results[pid] = {};
        if (!results[pid][fk]) results[pid][fk] = {};

        let { time, result } = await fileHandler.readIn({
          queryName: dirQueries.knn,
          filename,
        });

        timestemps[pid][fk][fileHandler.database] = time;
        results[pid][fk][fileHandler.database] = result;

        // timestemps[filename][fileHandler.database] = time;
        // results[filename][fileHandler.database] = result;
      }
    }
  }

  // console.log({ timestemps });
  // console.log({ results });
  return { timestemps, results };
}

await getKNNResults();

export async function getKClosestPairs() {
  let fileHandlers = await availbleFileHandlers();
  let timestemps = {};
  let results = {};

  // console.log(fileHandlers);

  for (let k of ks) {
    for (let fileHandler of fileHandlers) {
      // let filename = fileHandler.kClosestPairsFileName({ k });
      let filename = fileHandler.kClosestPairFileName({ k });
      let keyfile = filename.split('.')[0];
      let [_, fk] = keyfile.split('_');

      if (!timestemps[fk]) timestemps[fk] = {};
      if (!results[fk]) results[fk] = {};

      let { time, result } = await fileHandler.readIn({
        queryName: dirQueries.kClosestPair,
        filename,
      });

      timestemps[fk][fileHandler.database] = time;
      results[fk][fileHandler.database] = result;
    }
  }

  // exit(0);

  // console.log({ timestemps });
  // console.log({ results });
  return { timestemps, results };
}

await getKClosestPairs();

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
