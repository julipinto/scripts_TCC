// MATCH (node1:Node {id: 'ID1'})
// MATCH (node2:Node {id: 'ID2'})
// RETURN point.distance(node2.location, node2.location) AS dist;

import Neo4jConnection from '../connections/Neo4jConnection.js';
import { ks, node_pairs, radius } from '../utils/params.js';
import FileHandler, { dirQueries } from '../utils/FileHandler.js';

const fileHandler = new FileHandler('neo4j');

const client = new Neo4jConnection({
  database: 'neo4j',
  user: 'neo4j',
  password: 'root1234',
});

await client.connect();

// match (node1 {id: 4662482749})
// match (node2 {id: 7410560799})
// return point.distance(node1.location, node2.location)

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
    `MATCH (node:POINT {id: ${node1}}) ` +
    'MATCH (node2:POINT) ' +
    `WHERE point.distance(node.location, node2.location) < ${radius} ` +
    'RETURN node2;',
  windowRange: ({ node1, node2 }) =>
    'MATCH (node1:POINT) ' +
    `MATCH (b1:POINT {id: ${node1}}) ` +
    `MATCH (b2:POINT {id: ${node2}}) ` +
    'WHERE point.withinBBox(node1.location, b1.location, b2.location) OR point.withinBBox(node1.location, b2.location, b1.location) ' +
    'RETURN node1;',
  radiusCount: ({ node1 }, radius) =>
    `MATCH (node:POINT {id: ${node1}}) ` +
    'MATCH (node2:POINT) ' +
    `WHERE point.distance(node.location, node2.location) < ${radius} ` +
    'RETURN count(node2) AS nodeCount;',
  windowCount: ({ node1, node2 }) =>
    'MATCH (node1:POINT) ' +
    `MATCH (b1:POINT {id: ${node1}}) ` +
    `MATCH (b2:POINT {id: ${node2}}) ` +
    'WHERE point.withinBBox(node1.location, b1.location, b2.location) ' +
    'RETURN count(node1) AS nodeCount;',
  closestPair: ({ node1 }) =>
    "MATCH (node1:POINT {power: 'tower'}) " +
    "MATCH (node2:POINT {power: 'tower'}) " +
    'WHERE node1.id <> node2.id ' +
    'WITH node1, node2 ' +
    'ORDER BY point.distance(node1.location, node2.location) ' +
    'LIMIT 1 ' +
    'RETURN {node1: node1, node2: node2, distance: point.distance(b1.location, b2.location)}; ',
  knn: ({ node1 }, k) =>
    `MATCH (target:POINT {id: ${node1}}) ` +
    'MATCH (other:POINT) ' +
    'WHERE target.id <> other.id  ' +
    'WITH target, other ' +
    'ORDER BY point.distance(other.location, target.location) ' +
    `LIMIT ${k} ` +
    'RETURN other, point.distance(other.location, target.location) AS distance; ',
};

// MATCH (node1:POINT)
// MATCH (b1:POINT {id: 4662482749})
// MATCH (b2:POINT {id: 7410560799})
// WHERE point.withinBBox(node1.location, b1.location, b2.location) OR point.withinBBox(node1.location, b2.location, b1.location)
// RETURN node1;

// MATCH (node1:POINT)
// MATCH (b1:POINT {id: 4662482749})
// MATCH (b2:POINT {id: 7410560799})
// WHERE point.withinBBox(node1.location, b1.location, b2.location) OR point.withinBBox(node1.location, b2.location, b1.location)
// RETURN count(node1) AS nodeCount;

async function queryDistance() {
  // console.log(node_pairs[0]);
  // const query = queries.distance(node_pairs[0]);
  // const { result } = await client.query(query);
  // console.log(result.records.map((r) => r.get('distance'))[0]);
  let f = [];
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
    // f.push(r);
    // distances.push();
  }

  // await fileHandler.write(ks, distances);
}

// await queryDistance();

async function teste() {
  const { result } = await client.query(
    'WITH point({longitude: 0, latitude: 0}) AS point1, point({longitude: 180, latitude: 0}) AS point2 RETURN point.distance(point1, point2) / pi() AS distance;'
  );
  console.log(result.records.map((r) => r.get('distance'))[0]);
}

await teste();

await client.close();
