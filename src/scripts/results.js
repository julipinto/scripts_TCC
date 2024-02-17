import { readdir } from 'fs/promises';
// import { runAllMySQL } from './mysql.js';
// import { runAllPostgres } from './postgres.js';
// import { runAllMongodb } from './mongodb.js';
// import { runAllNeo4j } from './neo4j.js';

import FileHandler, { dirQueries } from '../utils/FileHandler.js';
import { node_pairs, ks, radius } from '../utils/params.js';
import { districtsFeatures } from '../utils/districtsPolygonHandler.js';

// export async function runAll() {
//   await runAllMySQL();
//   console.log('');
//   await runAllPostgres();
//   console.log('');
//   await runAllMongodb();
//   console.log('');
//   await runAllNeo4j();
// }

export async function availbleFileHandlers(out = 'out') {
  // let dirs = await readdir('./out');
  let dirs = await readdir(out);
  if (dirs.length === 0) throw new Error('No files found in ./out');

  return dirs.map((dir) => {
    return new FileHandler(dir, out);
  });
}

export async function getDistanceResults(out = 'out') {
  let fileHandlers = await availbleFileHandlers(out);
  let timestemps = {};
  let results = {};

  for (let pair of node_pairs) {
    for (let fileHandler of fileHandlers) {
      // r[fileHandler.database] = {};
      let filename = fileHandler.distanceFileName(pair);
      let keyfile = filename.split('.')[0];
      if (!timestemps[keyfile]) timestemps[keyfile] = {};
      if (!results[keyfile]) results[keyfile] = {};

      let res = await fileHandler.readIn({
        queryName: dirQueries.distance,
        filename,
      });

      if (!res) continue;

      timestemps[keyfile][fileHandler.database] = res.time;
      results[keyfile][fileHandler.database] = res.result;
    }
  }

  // console.log({ timestemps });
  // console.log({ results });
  return { timestemps, results };
}

export async function getRadiusResults(out = 'out') {
  let fileHandlers = await availbleFileHandlers(out);
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

        let res = await fileHandler.readIn({
          queryName: dirQueries.radius,
          filename,
        });

        if (!res) continue;

        timestemps[pid][fr][fileHandler.database] = res.time;
        results[pid][fr][fileHandler.database] = res.result;
      }
    }
  }
  // console.log({ timestemps });
  // console.log({ results });

  // console.log(Object.keys(timestemps).length);
  return { timestemps, results };
}

// await getRadiusResults(out);

export async function getWindowResults(out = 'out') {
  let fileHandlers = await availbleFileHandlers(out);
  let timestemps = {};
  let results = {};

  for (let pair of node_pairs) {
    for (let fileHandler of fileHandlers) {
      let filename = fileHandler.windowRQFileName(pair);
      let keyfile = filename.split('.')[0];
      if (!timestemps[keyfile]) timestemps[keyfile] = {};
      if (!results[keyfile]) results[keyfile] = {};

      let res = await fileHandler.readIn({
        queryName: dirQueries.window,
        filename,
      });

      if (!res) continue;

      timestemps[keyfile][fileHandler.database] = res.time;
      results[keyfile][fileHandler.database] = res.result;
    }
  }

  // console.log({ timestemps });
  // console.log(results['windowRQ_7410560799_4662482749']);
  return { timestemps, results };
}

// await getWindowResults(out);

export async function getRangeCountResults(out = 'out') {
  let fileHandlers = await availbleFileHandlers(out);

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

      let res = await fileHandler.readIn({
        queryName: dirQueries.windowCount,
        filename,
      });

      if (!res) continue;

      timestempsWindow[keyfile][fileHandler.database] = res.time;
      resultsWindow[keyfile][fileHandler.database] = res.result;

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

        let res = await fileHandler.readIn({
          queryName: dirQueries.radiusCount,
          filename,
        });

        if (!res) continue;

        timestempsRadius[pid][fr][fileHandler.database] = res.time;
        resultsRadius[pid][fr][fileHandler.database] = res.result;
      }
    }
  }

  // console.log({ timestempsWindow });
  // console.log({ resultsWindow });
  // console.log({ timestempsRadius });
  // console.log({ resultsRadius });
  return { timestempsWindow, resultsWindow, timestempsRadius, resultsRadius };
}

// await getRangeCountResults(out);

export async function getKNNResults(out = 'out') {
  let fileHandlers = await availbleFileHandlers(out);
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

        let res = await fileHandler.readIn({
          queryName: dirQueries.knn,
          filename,
        });

        if (!res) continue;

        timestemps[pid][fk][fileHandler.database] = res.time;
        results[pid][fk][fileHandler.database] = res.result;

        // timestemps[filename][fileHandler.database] = time;
        // results[filename][fileHandler.database] = result;
      }
    }
  }

  // console.log({ timestemps });
  // console.log({ results });
  return { timestemps, results };
}

// await getKNNResults(out);

export async function getKClosestPairs(out = 'out') {
  let fileHandlers = await availbleFileHandlers(out);
  let timestemps = {};
  let results = {};

  for (let k of ks) {
    for (let fileHandler of fileHandlers) {
      // let filename = fileHandler.kClosestPairsFileName({ k });
      let filename = fileHandler.kClosestPairFileName({ k });
      let keyfile = filename.split('.')[0];
      let [_, fk] = keyfile.split('_');

      if (!timestemps[fk]) timestemps[fk] = {};
      if (!results[fk]) results[fk] = {};

      let res = await fileHandler.readIn({
        queryName: dirQueries.kClosestPair,
        filename,
      });

      if (!res) continue;

      timestemps[fk][fileHandler.database] = res.time;
      results[fk][fileHandler.database] = res.result;
    }
  }

  // exit(0);

  // console.log({ timestemps });
  // console.log({ results });
  return { timestemps, results };
}

export async function getSpatialJoin(out = 'out') {
  let fileHandlers = await availbleFileHandlers(out);
  let timestemps = {};
  let results = {};

  for (const feature of districtsFeatures) {
    for (const fileHandler of fileHandlers) {
      let filename = fileHandler.spatialJoinFileName({
        district: feature.properties.district,
      });

      let keyfile = filename.split('.')[0];
      let [_, fk] = keyfile.split('_');

      if (!timestemps[fk]) timestemps[fk] = {};
      if (!results[fk]) results[fk] = {};

      let res = await fileHandler.readIn({
        queryName: dirQueries.spatialJoin,
        filename,
      });

      if (!res) continue;

      timestemps[fk][fileHandler.database] = res.time;
      results[fk][fileHandler.database] = res.result;
    }
  }

  return { timestemps, results };
}

// await getKClosestPairs();

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
// console.log(await getKClosestPairs());
