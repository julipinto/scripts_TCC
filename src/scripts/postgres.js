import PostgresConnection from '../connections/PostgresConnection.js';
import { ks, node_pairs, radius, tagClosestPair } from '../utils/params.js';
import FileHandler, { dirQueries } from '../utils/FileHandler.js';
import { removeDuplicates } from '../utils/removeKCPDuplucates.js';
import {
  districtsFeatures,
  polygonToCoordinates,
} from '../utils/districtsPolygonHandler.js';

const fileHandler = new FileHandler('postgres');

const client = new PostgresConnection({
  database: 'map',
  user: 'root',
  password: 'root',
  hostname: 'localhost'
});

const SRID = 0;

const queries = {
  distance: ({ node1, node2 }) => `SELECT
  ST_DistanceSphere(
    (SELECT location FROM nodes WHERE node_id = ${node1}),
    (SELECT location FROM nodes WHERE node_id = ${node2})
  ) AS distance;`,

  radiusRange: ({ node1 }, radius) => `SELECT n.node_id, n.location
  FROM nodes n
  JOIN node_tags nt ON n.node_id = nt.node_id
  WHERE (nt.tag_key = 'shop' OR nt.tag_key = 'amenity')
  AND ST_DWithin(n.location, (SELECT location FROM nodes WHERE node_id = ${node1}), ${radius}, true);`,

  windowRange: ({ node1, node2 }) => `SELECT n.node_id,  n.location
    FROM nodes n
    JOIN node_tags nt ON n.node_id = nt.node_id
    WHERE (nt.tag_key = 'shop' OR nt.tag_key = 'amenity')
    AND ST_Within(
      n.location,
      ST_MakeEnvelope(
        (SELECT ST_X(location) FROM nodes WHERE node_id = ${node1}),
        (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node1}),
        (SELECT ST_X(location) FROM nodes WHERE node_id = ${node2}),
        (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node2}),
        ${SRID})::geometry
    )`,

  radiusRangeCount: ({ node1 }, radius) => `SELECT COUNT(*) AS count
    FROM nodes n
    JOIN node_tags nt ON n.node_id = nt.node_id
    WHERE (nt.tag_key = 'shop' OR nt.tag_key = 'amenity')
    AND ST_DWithin(n.location, (SELECT location FROM nodes WHERE node_id = ${node1}), ${radius}, true);`,

  windowRangeCount: ({ node1, node2 }) => `SELECT COUNT(*) AS count
    FROM nodes n
    JOIN node_tags nt ON n.node_id = nt.node_id
    WHERE (nt.tag_key = 'shop' OR nt.tag_key = 'amenity')
    AND ST_Within(
      n.location,
      ST_MakeEnvelope(
        (SELECT ST_X(location) FROM nodes WHERE node_id = ${node1}),
        (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node1}),
        (SELECT ST_X(location) FROM nodes WHERE node_id = ${node2}),
        (SELECT ST_Y(location) FROM nodes WHERE node_id = ${node2}),
        ${SRID})::geometry
    )`,

  knn: (
    { node1 },
    k
  ) => `SELECT n.node_id AS neighbor_id, n.location AS neighbor_location,
    ST_DistanceSphere(n.location, (SELECT location FROM nodes WHERE node_id = ${node1})) AS distance
    FROM nodes n
    JOIN node_tags nt ON n.node_id = nt.node_id
    WHERE n.node_id != ${node1} AND nt.tag_key = 'amenity' AND nt.tag_value = 'restaurant'
    ORDER BY distance
    LIMIT ${k};`,

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
    'ST_DistanceSphere(nt1.location, nt2.location) AS distance ' +
    'FROM tagged_nodes nt1 ' +
    'CROSS JOIN tagged_nodes nt2 ' +
    'WHERE nt1.node_id != nt2.node_id ' +
    'ORDER BY distance ASC ' +
    `LIMIT ${k};`,

  spatialJoin: (polygon_points) => `
    SELECT n.node_id
    FROM nodes n JOIN node_tags nt ON n.node_id = nt.node_id
    WHERE (nt.tag_key='amenity' OR nt.tag_key='shop')
    AND ST_Within(n.location,  ST_GeomFromText('POLYGON((${polygon_points}))', 0));`,
};

async function queryDistance() {
  // let { result } = await client.query(queries.distance(node_pairs[0]));
  // let re = [];

  console.time('Query All Distance');
  for (let pair of node_pairs) {
    let { result, time } = await client.query(queries.distance(pair));
    // console.log(result);
    // re.push(result.rows[0].distance);
    fileHandler.writeOut({
      queryName: dirQueries.distance,
      filename: fileHandler.distanceFileName(pair),
      data: { time, result: result.rows[0].distance },
    });
  }
  console.timeEnd('Query All Distance');
  // console.log(re.join('\n'));
}

async function queryRadiusRange() {
  console.time('Query All Radius Range');
  for (let pair of node_pairs) {
    for (let r of radius) {
      let { result, time } = await client.query(queries.radiusRange(pair, r));
      fileHandler.writeOut({
        queryName: dirQueries.radius,
        filename: fileHandler.radiusRQFileName({ ...pair, radius: r }),
        data: { time, result: result.rows.map(({ node_id }) => node_id) },
      });
    }
  }
  console.timeEnd('Query All Radius Range');
}

async function queryWindowRange() {
  console.time('Query All Window Range');
  for (let pair of node_pairs) {
    let { result, time } = await client.query(queries.windowRange(pair));
    fileHandler.writeOut({
      queryName: dirQueries.window,
      filename: fileHandler.windowRQFileName(pair),
      data: { time, result: result.rows.map(({ node_id }) => node_id) },
    });
  }
  console.timeEnd('Query All Window Range');
}

async function queryRangeCount() {
  console.time('Query All Range Count');
  for (let pair of node_pairs) {
    let { result, time } = await client.query(queries.windowRangeCount(pair));
    fileHandler.writeOut({
      queryName: dirQueries.windowCount,
      filename: fileHandler.rangeCountFileName({
        ...pair,
        type: dirQueries.windowCount,
      }),
      data: { time, result: Number(result.rows[0].count) },
    });

    for (let r of radius) {
      let { result, time } = await client.query(
        queries.radiusRangeCount(pair, r)
      );
      fileHandler.writeOut({
        queryName: dirQueries.radiusCount,
        filename: fileHandler.rangeCountFileName({
          ...pair,
          type: dirQueries.radiusCount,
          radius: r,
        }),
        data: { time, result: Number(result.rows[0].count) },
      });
    }
  }
  console.timeEnd('Query All Range Count');
}

// await queryRangeCount();

async function queryKNN() {
  console.time('Query All KNN');
  for (let pair of node_pairs) {
    for (let k of ks) {
      let { result, time } = await client.query(queries.knn(pair, k));
      fileHandler.writeOut({
        queryName: dirQueries.knn,
        filename: fileHandler.knnFileName({ ...pair, k }),
        data: {
          time,
          result: result.rows.map(({ neighbor_id }) => neighbor_id),
        },
      });
    }
  }
  console.timeEnd('Query All KNN');
}

// await knn();

async function queryKClosestPair() {
  console.time('Query All K Closest Pair');
  for (let k of ks) {
    let query = queries.kClosestPair({
      ...tagClosestPair,
      k: k * 2,
    });

    let { time, result } = await client.query(query);
    let withoutDuplicates = removeDuplicates(result.rows);
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
      data: { time, result: result.rows.map(({ node_id }) => node_id) },
    });
  }
  console.timeEnd('Query All Spatial Join');
}

export async function runAllPostgres() {
  console.log('Running Postgres queries');
  await client.connect();
  await client.query('SELECT NOW();');
  await queryDistance();
  await queryRadiusRange();
  // await queryWindowRange();
  // await queryRangeCount();
  // await queryKNN();
  // await queryKClosestPair();
  // await querySpatialJoin();
  await client.close();
}

// await runAllPostgres();

// await queryDistance();
