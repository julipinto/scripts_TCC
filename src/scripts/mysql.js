import MysqlConnection from '../connections/MysqllConnection.js';
import { ks, node_pairs, radius } from '../utils/params.js';
import FileHandler, { dirQueries } from '../utils/fileHandler.js';
// r = {
//   time: 0,
//   result: {
//     rows: [],
//     ...
//   }

const fileHandler = new FileHandler('mysql');

const client = new MysqlConnection({
  database: 'map',
  user: 'root',
  password: 'root',
});

const queries = {
  distance: ({ node1, node2 }) =>
    'SELECT ' +
    'ST_Distance(' +
    `(SELECT location FROM nodes WHERE node_id = ${node1}),` +
    `(SELECT location FROM nodes WHERE node_id = ${node2})` +
    ') AS distance;',

  radiusRange: ({ node1 }, radius) =>
    'SELECT node_id, location FROM nodes ' +
    'WHERE ST_Distance_Sphere(location, ' +
    `(SELECT location FROM nodes WHERE node_id = ${node1})) <= ${radius};`,
  windowRange: ({ node1, node2 }) =>
    'SELECT n.node_id, n.location FROM nodes n ' +
    `WHERE ST_X(n.location) >= LEAST((SELECT ST_X(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_X(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_X(n.location) <= GREATEST((SELECT ST_X(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_X(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_Y(n.location) >= LEAST((SELECT ST_Y(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_Y(n.location) <= GREATEST((SELECT ST_Y(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND n.node_id != ${node1} AND n.node_id != ${node2};`,

  radiusRangeCount: ({ node1 }, radius) =>
    'SELECT COUNT(*) AS count FROM nodes ' +
    'WHERE ST_Distance_Sphere(location, ' +
    `(SELECT location FROM nodes WHERE node_id = ${node1})) <= ${radius};`,
  windowRangeCount: ({ node1, node2 }) =>
    'SELECT COUNT(*) AS count FROM nodes n ' +
    `WHERE ST_X(n.location) >= LEAST((SELECT ST_X(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_X(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_X(n.location) <= GREATEST((SELECT ST_X(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_X(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_Y(n.location) >= LEAST((SELECT ST_Y(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_Y(n.location) <= GREATEST((SELECT ST_Y(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND n.node_id != ${node1} AND n.node_id != ${node2};`,

  knn: ({ node1 }, k) =>
    'SELECT n.node_id,  n.location, ' +
    `ST_Distance_Sphere(n.location, (SELECT location FROM nodes WHERE node_id = ${node1})) AS distance ` +
    `FROM nodes n WHERE n.node_id != ${node1} ` +
    `ORDER BY distance LIMIT ${k};`,
};

await client.connect();
await client.query('SELECT NOW();');

// let {result} = await client.query('SELECT * FROM nodes ORDER BY RAND() LIMIT 40;');

// // // // // // // // // // distance
async function queryDistance() {
  // let times = [];
  // let results = [];
  for (let pair of node_pairs) {
    let { result, time } = await client.query(queries.distance(pair));
    let r = result[0][0].distance;
    let filename = fileHandler.distanceFileName(pair);
    fileHandler.writeOut({
      queryName: dirQueries.distance,
      filename,
      data: { time, result: r },
    });
  }
  // console.table({ mysql: times });
  // console.table({ mysql: results });
}

// // // // // // // // // // rrq
async function queryRadiusRange() {
  for (let pair of node_pairs) {
    for (let r of radius) {
      let { time, result } = await client.query(queries.radiusRange(pair, r));
      let filename = fileHandler.radiusRQFileName({ ...pair, radius: r });
      fileHandler.writeOut({
        queryName: dirQueries.radius,
        filename,
        data: { time, result: result[0].map(({ node_id }) => node_id) },
      });
    }
  }
}

// // // // // // // // // // wrq
async function queryWindowRange() {
  // let { result } = await client.query(queries.wrq(node_pairs[0]));
  // console.log(result);
  for (let pair of node_pairs) {
    let { time, result } = await client.query(queries.windowRange(pair));
    let filename = fileHandler.windowRQFileName(pair);
    fileHandler.writeOut({
      queryName: dirQueries.window,
      filename,
      data: { time, result: result[0].map(({ node_id }) => node_id) },
    });
  }
}

// await queryWindowRange();

async function queryRangeCount() {
  // let times = [];
  // let results = [];
  // console.log(
  //   (await client.query(queries.windowRangeCount(node_pairs[0]))).result[0][0]
  //     .count
  // );
  for (let pair of node_pairs) {
    let { time, result } = await client.query(queries.windowRangeCount(pair));
    let filename = fileHandler.rangeCountFileName({
      ...pair,
      type: dirQueries.windowCount,
    });
    fileHandler.writeOut({
      queryName: dirQueries.windowCount,
      filename,
      data: { time, result: result[0][0]['count'] },
    });

    for (let r of radius) {
      let { time, result } = await client.query(
        queries.radiusRangeCount(pair, r)
      );
      let filename = fileHandler.rangeCountFileName({
        ...pair,
        radius: r,
        type: dirQueries.radiusCount,
      });
      fileHandler.writeOut({
        queryName: dirQueries.radiusCount,
        filename,
        data: { time, result: result[0][0]['count'] },
      });
    }
  }
  // console.table({ mysql: times });
  // console.table({ mysql: results });
}

async function knn() {
  // let times = [];
  // let results = [];
  for (let pair of node_pairs) {
    for (let k of ks) {
      let { time, result } = await client.query(queries.knn(pair, k));
      let filename = fileHandler.knnFileName({ ...pair, k });
      fileHandler.writeOut({
        queryName: dirQueries.knn,
        filename,
        data: { time, result: result[0].map(({ node_id }) => node_id) },
      });
    }
  }
}

async function runAll() {
  await queryDistance();
  await queryRadiusRange();
  await queryWindowRange();
  await queryRangeCount();
  await knn();
}

// await runAll();

await client.close();
