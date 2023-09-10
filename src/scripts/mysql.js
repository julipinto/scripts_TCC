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
    'ST_Distance_Sphere(' +
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

  shortestPath: ({ node1: source_node_id, node2: target_node_id }) => {
    return {
      createTemporaryCostTable:
        'CREATE TEMPORARY TABLE costs (node_id BIGINT PRIMARY KEY, cost INT);',
      createTemporaryVisitedTable:
        'CREATE TEMPORARY TABLE visited (node_id BIGINT PRIMARY KEY, visited BOOLEAN);',
      defineSourceNode: `INSERT INTO costs (node_id, cost) SELECT node_id, CASE WHEN node_id = ${source_node_id} THEN 0 ELSE 999999 END FROM nodes;`,
      defineVisited: `INSERT INTO visited (node_id, visited) SELECT node_id, FALSE FROM nodes;`,
      loopAlgorithm:
        'WHILE (SELECT COUNT(*) FROM visited WHERE visited = FALSE) > 0 DO ' +
        'SET @current_node_id = (SELECT node_id FROM costs WHERE visited = FALSE ORDER BY cost LIMIT 1);' +
        'UPDATE visited SET visited = TRUE WHERE node_id = @current_node_id; ' +
        'UPDATE costs AS c ' +
        'JOIN way_nodes AS wn ON c.node_id = wn.node_id ' +
        'JOIN way_nodes AS wn2 ON wn.way_id = wn2.way_id AND wn2.node_id != wn.node_id ' +
        'JOIN nodes AS n ON wn2.node_id = n.node_id ' +
        'SET c.cost = LEAST(c.cost, (SELECT cost FROM costs WHERE node_id = @current_node_id) + 1) ' +
        'WHERE c.node_id = n.node_id AND visited = FALSE; ' +
        ` IF @current_node_id = ${target_node_id} THEN LEAVE;` +
        'END IF; ' +
        'END WHILE;',
    };
  },
  closestPair: ({ key, value }) => {
    'WITH tagged_nodes AS (' +
      'SELECT node_id, location ' +
      'FROM nodes ' +
      'WHERE node_id IN (' +
      'SELECT DISTINCT nt1.node_id ' +
      'FROM node_tags nt1 ' +
      `WHERE nt1.tag_key = '${key}' AND nt1.tag_value = '${value}' ` +
      ')' +
      ')' +
      'SELECT ' +
      'nt1.node_id AS node_id_1, ' +
      'nt2.node_id AS node_id_2, ' +
      'ST_Distance_Sphere(nt1.location, nt2.location) AS distance ' +
      'FROM tagged_nodes nt1 ' +
      'CROSS JOIN tagged_nodes nt2 ' +
      'WHERE nt1.node_id != nt2.node_id ' +
      'ORDER BY distance ASC ' +
      'LIMIT 1; ';
  },
};

await client.connect();
await client.query('SELECT NOW();');

// let {result} = await client.query('SELECT * FROM nodes ORDER BY RAND() LIMIT 40;');

// // // // // // // // // // distance
async function queryDistance() {
  // let times = [];
  // let results = [];
  // let re = [];
  for (let pair of node_pairs) {
    let { result, time } = await client.query(queries.distance(pair));
    // let r = result[0][0].distance;
    // re.push(r);
    let filename = fileHandler.distanceFileName(pair);
    fileHandler.writeOut({
      queryName: dirQueries.distance,
      filename,
      data: { time, result: r },
    });
  }
  console.log(re.join('\n'));
  // console.table({ mysql: times });
  // console.table({ mysql: results });
}

// await queryDistance();

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

async function queryKNN() {
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

async function shortestPath() {
  let shortestPath = queries.shortestPath(node_pairs[0]);

  try {
    let r1 = await client.query(shortestPath.createTemporaryCostTable);
    console.log(r1);
    let r2 = await client.query(shortestPath.createTemporaryVisitedTable);
    console.log(r2);
    let r3 = await client.query(shortestPath.defineSourceNode);
    console.log(r3);
    let r4 = await client.query(shortestPath.defineVisited);
    console.log(r4);
    let r5 = await client.query(shortestPath.loopAlgorithm);
    console.log(r5);
  } catch (error) {
    console.error(error);
  }
}

// await shortestPath();

// async function runAll() {
//   await queryDistance();
//   await queryRadiusRange();
//   await queryWindowRange();
//   await queryRangeCount();
//   await knn();
// }

// let { result } = await client.query(
//   'SELECT ST_Distance_Sphere(POINT(-74.0060, 40.7128), POINT(-118.2437, 34.0522)) AS distance;'
// );
// console.table(result[0]);

// await runAll();

await client.close();
