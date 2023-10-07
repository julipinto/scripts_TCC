import MysqlConnection from '../connections/MysqlConnection.js';
import { ks, node_pairs, radius, tagClosestPair } from '../utils/params.js';
import FileHandler, { dirQueries } from '../utils/FileHandler.js';
import { removeDuplicates } from '../utils/removeKCPDuplucates.js';
import {
  districtsFeatures,
  polygonToCoordinates,
} from '../utils/districtsPolygonHandler.js';

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
    'SELECT n.node_id, n.location FROM nodes n ' +
    'JOIN node_tags nt ON n.node_id = nt.node_id ' +
    `WHERE (nt.tag_key='amenity' OR nt.tag_key='shop') ` +
    `AND ST_Distance_Sphere(location, (SELECT location FROM nodes WHERE node_id = ${node1})) <= ${radius}; `,
  windowRange: ({ node1, node2 }, { tag_key, tag_value }) =>
    'SELECT n.node_id, n.location FROM nodes n ' +
    'JOIN node_tags nt ON n.node_id = nt.node_id ' +
    `WHERE (nt.tag_key = 'amenity' OR nt.tag_key = 'shop') ` +
    `AND ST_X(n.location) >= LEAST((SELECT ST_X(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_X(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_X(n.location) <= GREATEST((SELECT ST_X(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_X(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_Y(n.location) >= LEAST((SELECT ST_Y(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_Y(n.location) <= GREATEST((SELECT ST_Y(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND n.node_id != ${node1} AND n.node_id != ${node2};`,

  radiusRangeCount: ({ node1 }, radius) =>
    'SELECT COUNT(*) as count FROM nodes n ' +
    'JOIN node_tags nt ON n.node_id = nt.node_id ' +
    `WHERE (nt.tag_key='amenity' OR nt.tag_key='shop') ` +
    `AND ST_Distance_Sphere(location, (SELECT location FROM nodes WHERE node_id = ${node1})) <= ${radius}; `,
  windowRangeCount: ({ node1, node2 }) =>
    'SELECT COUNT(*) AS count FROM nodes n ' +
    'JOIN node_tags nt ON n.node_id = nt.node_id ' +
    `WHERE (nt.tag_key = 'amenity' OR nt.tag_key = 'shop') ` +
    `AND ST_X(n.location) >= LEAST((SELECT ST_X(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_X(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_X(n.location) <= GREATEST((SELECT ST_X(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_X(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_Y(n.location) >= LEAST((SELECT ST_Y(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND ST_Y(n.location) <= GREATEST((SELECT ST_Y(location) FROM nodes WHERE node_id = ${node1}), (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node2})) ` +
    `AND n.node_id != ${node1} AND n.node_id != ${node2};`,

  knn: ({ node1 }, k) =>
    'SELECT n.node_id,  n.location, ' +
    `ST_Distance_Sphere(n.location, (SELECT location FROM nodes WHERE node_id = ${node1})) AS distance ` +
    `FROM nodes n JOIN node_tags nt ON n.node_id = nt.node_id ` +
    `WHERE n.node_id != ${node1} AND nt.tag_key = 'amenity' AND nt.tag_value = 'restaurant' ` +
    `ORDER BY distance LIMIT ${k};`,

  kClosestPair: ({ key, value, k }) =>
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
    'nt1.node_id AS node_id1, ' +
    'nt2.node_id AS node_id2, ' +
    'ST_Distance_Sphere(nt1.location, nt2.location) AS distance ' +
    'FROM tagged_nodes nt1 ' +
    'CROSS JOIN tagged_nodes nt2 ' +
    'WHERE nt1.node_id != nt2.node_id ' +
    'ORDER BY distance ASC ' +
    `LIMIT ${k};`,
  spatialJoin: (polygon_points) =>
    'SELECT n.node_id ' +
    'FROM nodes n JOIN node_tags nt ON n.node_id = nt.node_id ' +
    "WHERE (nt.tag_key='amenity' OR nt.tag_key='shop') " +
    `AND ST_Intersects(n.location,  ST_GeomFromText('POLYGON((${polygon_points}))', 0));`,
};

// let {result} = await client.query('SELECT * FROM nodes ORDER BY RAND() LIMIT 40;');

// // // // // // // // // // distance
async function queryDistance() {
  console.time('Query All Distance');
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
  console.timeEnd('Query All Distance');
}

// await queryDistance();

// // // // // // // // // // rrq
async function queryRadiusRange() {
  console.time('Query All Radius Range');
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
  console.timeEnd('Query All Radius Range');
}

// // // // // // // // // // wrq
async function queryWindowRange() {
  console.time('Query All Window Range');
  let tag = { tag_key: 'amenity', tag_value: 'restaurant' };
  for (let pair of node_pairs) {
    let { time, result } = await client.query(queries.windowRange(pair, tag));
    let filename = fileHandler.windowRQFileName(pair);
    fileHandler.writeOut({
      queryName: dirQueries.window,
      filename,
      data: { time, result: result[0].map(({ node_id }) => node_id) },
    });
  }
  console.timeEnd('Query All Window Range');
}

// await queryWindowRange();

async function queryRangeCount() {
  console.time('Query All Range Count');
  for (let pair of node_pairs) {
    // ////////////// window
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

    // ////////////// radius
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
  console.timeEnd('Query All Range Count');
}

async function queryKNN() {
  console.time('Query All KNN');
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
  console.timeEnd('Query All KNN');
}

async function queryKClosestPair() {
  console.time('Query All K Closest Pair');
  for (let k of ks) {
    let query = queries.kClosestPair({
      ...tagClosestPair,
      k: k * 2,
    });

    let { time, result } = await client.query(query);

    let withoutDuplicates = removeDuplicates(result[0]);

    let filename = fileHandler.kClosestPairFileName({ k });

    fileHandler.writeOut({
      queryName: dirQueries.kClosestPair,
      filename,
      data: { time, result: withoutDuplicates },
    });
  }
  console.timeEnd('Query All K Closest Pair');
}

async function querySpatialJoin() {
  console.time('Query All Spatial Join');
  for (const district_feature of districtsFeatures) {
    let { district, coordinates } = polygonToCoordinates(district_feature);
    let query = queries.spatialJoin(coordinates);

    let { time, result } = await client.query(query);

    let filename = fileHandler.spatialJoinFileName({ district });

    fileHandler.writeOut({
      queryName: dirQueries.spatialJoin,
      filename,
      data: { time, result: result[0].map(({ node_id }) => node_id) },
    });
  }
  console.timeEnd('Query All Spatial Join');
}

export async function runAllMySQL() {
  console.log('Running MySQL queries');
  await client.connect();
  await client.query('SELECT NOW();');
  await queryDistance();
  await queryRadiusRange();
  await queryWindowRange();
  await queryRangeCount();
  await queryKNN();
  await queryKClosestPair();
  await querySpatialJoin();
  await client.close();
}
