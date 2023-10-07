// MATCH (node1:Node {id: 'ID1'})
// MATCH (node2:Node {id: 'ID2'})
// RETURN point.distance(node2.location, node2.location) AS dist;

import Neo4jConnection from '../connections/Neo4jConnection.js';
import { ks, node_pairs, radius, tagClosestPair } from '../utils/params.js';
import FileHandler, { dirQueries } from '../utils/FileHandler.js';
import { removeDuplicates } from '../utils/removeKCPDuplucates.js';
import {
  districtsFeatures,
  polygonToMysql,
} from '../utils/districtsPolygonHandler.js';

const fileHandler = new FileHandler('neo4j');

const client = new Neo4jConnection({
  database: 'neo4j',
  user: 'neo4j',
  password: 'root1234',
});

// await client.connect();

const queries = {
  // distance: ({ node1, node2 }) =>
  //   `MATCH (node1:POINT {id: ${node1}}) ` +
  //   `MATCH (node2:POINT {id: ${node2}}) ` +
  //   'RETURN point.distance(node1.location, node2.location) AS distance;',
  distance: ({ node1, node2 }) =>
    `MATCH (node1:POINT {id: ${node1}}) ` +
    `MATCH (node2:POINT {id: ${node2}}) ` +
    'RETURN point.distance(node1.location, node2.location) AS distance;',
  radiusRange: ({ node1 }, radius) =>
    `MATCH (source:POINT {id: ${node1}}) ` +
    'MATCH (target:POINT) ' +
    'WHERE (target.shop IS NOT NULL OR target.amenity IS NOT NULL) ' +
    `AND point.distance(source.location, target.location) < ${radius} ` +
    'RETURN target;',
  windowRange: ({ node1, node2 }) =>
    'MATCH (target:POINT) ' +
    `MATCH (source1:POINT {id: ${node1}}) ` +
    `MATCH (source2:POINT {id: ${node2}}) ` +
    'WHERE (target.shop IS NOT NULL OR target.amenity IS NOT NULL) ' +
    'AND point.withinBBox(target.location, source1.location, source2.location) ' +
    'RETURN target;',
  radiusCount: ({ node1 }, radius) =>
    `MATCH (source:POINT {id: ${node1}}) ` +
    'MATCH (target:POINT) ' +
    'WHERE (target.shop IS NOT NULL OR target.amenity IS NOT NULL) ' +
    `AND point.distance(source.location, target.location) < ${radius} ` +
    'RETURN count(node2) AS nodeCount;',
  windowCount: ({ node1, node2 }) =>
    'MATCH (node1:POINT) ' +
    `MATCH (b1:POINT {id: ${node1}}) ` +
    `MATCH (b2:POINT {id: ${node2}}) ` +
    'WHERE (target.shop IS NOT NULL OR target.amenity IS NOT NULL) ' +
    'AND point.withinBBox(target.location, source1.location, source2.location) ' +
    'RETURN count(node1) AS nodeCount;',
  kClosestPair: ({ key, value, k }) =>
    `MATCH (node1:POINT {${key}: '${value}'}) ` +
    `MATCH (node2:POINT {${key}: '${value}'}) ` +
    'WHERE node1.id <> node2.id ' +
    'WITH node1, node2 ' +
    'ORDER BY point.distance(node1.location, node2.location) ' +
    `LIMIT ${k} ` +
    'RETURN node1, node2, point.distance(node1.location, node2.location) AS distance; ',
  // 'RETURN {node1: node1, node2: node2, distance: point.distance(node1.location, node2.location)}; ',
  knn: ({ node1 }, k) =>
    `MATCH (source:POINT {id: ${node1}}) ` +
    "MATCH (target:POINT {amenity: 'restaurant'}) " +
    'WHERE source.id <> target.id  ' +
    'WITH source, target ' +
    'ORDER BY point.distance(target.location, source.location) ' +
    `LIMIT ${k} ` +
    'RETURN target; ',
  // 'RETURN target, point.distance(target.location, source.location) AS distance; ',
  // spatialJoin: (polygon_points) =>
  // `WITH "POLYGON((${polygon_points}))" as polygon ` +
  // 'MATCH (target:POINT) ' +
  // 'WHERE (target.shop IS NOT NULL OR target.amenity IS NOT NULL) ' +
  // // 'AND amanzi.withinPolygon(polygon, target.location) ' +
  // // 'AND spatial.intersects(polygon, target.location) ' +
  // 'AND target.location IN polygon ' +
  // 'RETURN target',
};

async function queryDistance() {
  console.time('Query All Distance');
  for (const pair of node_pairs) {
    const query = queries.distance(pair);
    const { result, time } = await client.query(query);
    let r = result.records.map((r) => r.get('distance'))[0];
    let filename = fileHandler.distanceFileName(pair);
    fileHandler.writeOut({
      queryName: dirQueries.distance,
      filename,
      data: { time, result: r },
    });
  }
  console.timeEnd('Query All Distance');
}

async function queryRadiusRange() {
  console.time('Query All Radius Range');
  for (const pair of node_pairs) {
    for (const r of radius) {
      const query = queries.radiusRange(pair, r);
      const { result, time } = await client.query(query);
      let res = result.records.map((r) =>
        r.get('target').properties.id.toNumber()
      );

      let filename = fileHandler.radiusRQFileName({ ...pair, radius: r });
      fileHandler.writeOut({
        queryName: dirQueries.radius,
        filename,
        data: { time, result: res },
      });
    }
  }
  console.timeEnd('Query All Radius Range');
}

async function queryWindowRange() {
  console.time('Query All Window Range');
  for (const pair of node_pairs) {
    const query = queries.windowRange(pair);
    const { result, time } = await client.query(query);
    let res = result.records.map((r) =>
      r.get('target').properties.id.toNumber()
    );

    let filename = fileHandler.windowRQFileName(pair);
    fileHandler.writeOut({
      queryName: dirQueries.window,
      filename,
      data: { time, result: res },
    });
  }
  console.timeEnd('Query All Window Range');
}

async function queryRangeCount() {
  console.time('Query All Radius Count');
  for (const pair of node_pairs) {
    let { time, result } = await client.query(queries.windowCount(pair));
    let filename = fileHandler.rangeCountFileName({
      ...pair,
      type: dirQueries.windowCount,
    });

    fileHandler.writeOut({
      queryName: dirQueries.windowCount,
      filename,
      data: { time, result: result.records[0].get('nodeCount').toNumber() },
    });

    for (const r of radius) {
      let { time, result } = await client.query(queries.radiusCount(pair, r));
      let filename = fileHandler.rangeCountFileName({
        ...pair,
        type: dirQueries.radiusCount,
        radius: r,
      });

      fileHandler.writeOut({
        queryName: dirQueries.radiusCount,
        filename,
        data: { time, result: result.records[0].get('nodeCount').toNumber() },
      });
    }
  }
  console.timeEnd('Query All Radius Count');
}

async function queryKNN() {
  console.time('Query All KNN');
  for (const pair of node_pairs) {
    for (const k of ks) {
      const query = queries.knn(pair, k);
      let { time, result } = await client.query(query);
      let res = result.records.map((r) =>
        r.get('target').properties.id.toNumber()
      );

      let filename = fileHandler.knnFileName({ ...pair, k });
      fileHandler.writeOut({
        queryName: dirQueries.knn,
        filename,
        data: { time, result: res },
      });
    }
  }
  console.timeEnd('Query All KNN');
}

async function queryKClosestPair() {
  console.time('Query All K Closest Pair');
  for (const k of ks) {
    const query = queries.kClosestPair({ ...tagClosestPair, k: k * 2 });
    let { time, result } = await client.query(query);
    let res = result.records.map((r) => ({
      node_id1: r.get('node1').properties.id.toNumber(),
      node_id2: r.get('node2').properties.id.toNumber(),
      distance: r.get('distance'),
    }));
    let withoutDuplicates = removeDuplicates(res);
    let filename = fileHandler.kClosestPairFileName({ k });
    fileHandler.writeOut({
      queryName: dirQueries.kClosestPair,
      filename,
      data: { time, result: withoutDuplicates },
    });
  }
  console.timeEnd('Query All K Closest Pair');
}

// async function querySpatialJoin() {
//   console.time('Query All Spatial Join');
//   for (const district_feature of districtsFeatures) {
//     let { district, coordinates } = polygonToMysql(district_feature);
//     let query = queries.spatialJoin(coordinates);

//     let { time, result } = await client.query(query);

//     let filename = fileHandler.spatialJoinFileName({ district });

//     console.log(result.records.map((r) => r.get('polygon')));
// fileHandler.writeOut({
//   queryName: dirQueries.spatialJoin,
//   filename,
//   data: {
//     time,
//     result: result.records.map((r) =>
//       r.get('target').properties.id.toNumber()
//     ),
//   },
// });
// }
// console.timeEnd('Query All Spatial Join');
// }

export async function runAllNeo4j() {
  console.log('Running Neo4j queries');
  await client.connect();
  await client.query('RETURN timestamp() AS currentTimestamp;');
  // await queryDistance();
  // await queryRadiusRange();
  // await queryWindowRange();
  // await queryRangeCount();
  // await queryKNN();
  // await queryKClosestPair();
  await client.close();
}

await runAllNeo4j();
