import SurrealdbConnection from '../connections/SurrealdbConnection.js';
import { ks, node_pairs, radius, tagClosestPair } from '../utils/params.js';
import FileHandler, { dirQueries } from '../utils/FileHandler.js';
import { removeDuplicates } from '../utils/removeKCPDuplucates.js';

const fileHandler = new FileHandler('surrealdb');

const client = new SurrealdbConnection({
  database: 'map',
  user: 'root',
  password: 'root',
});

const queries = {
  distance: ({ node1, node2 }) => `
    LET $p1 = (SELECT location FROM nodes:${node1})[0].location;
    LET $p2 = (SELECT location FROM nodes:${node2})[0].location;
    RETURN geo::distance($p1, $p2);`,

  radiusRange: ({ node1 }, radius) => `
    LET $source = (SELECT location FROM nodes:${node1})[0].location;
    SELECT id FROM nodes WHERE geo::distance(location, $source) <= ${radius};
  `,
  windowRange: ({ node1, node2 }) => `
    LET $x1 = (SELECT location FROM nodes:${node1})[0].location.coordinates[0];
    LET $y1 = (SELECT location FROM nodes:${node1})[0].location.coordinates[1];
    LET $x2 = (SELECT location FROM nodes:${node2})[0].location.coordinates[0];
    LET $y2 = (SELECT location FROM nodes:${node2})[0].location.coordinates[1];
    SELECT id FROM nodes
    WHERE
      location.coordinates[0] >= math::min([$x1, $x2]) AND
      location.coordinates[0] <= math::max([$x1, $x2]) AND
      location.coordinates[1] >= math::min([$y1, $y2]) AND
      location.coordinates[1] <= math::max([$y1, $y2]) AND
      ${node1} != ${node2};
  `,

  radiusRangeCount: ({ node1 }, radius) => `
    LET $source = (SELECT location FROM nodes:${node1})[0].location;
    COUNT(SELECT id FROM nodes WHERE geo::distance(location, $source) <= ${radius});
  `,
  windowRangeCount: ({ node1, node2 }) => `
    LET $x1 = (SELECT location FROM nodes:${node1})[0].location.coordinates[0];
    LET $y1 = (SELECT location FROM nodes:${node1})[0].location.coordinates[1];
    LET $x2 = (SELECT location FROM nodes:${node2})[0].location.coordinates[0];
    LET $y2 = (SELECT location FROM nodes:${node2})[0].location.coordinates[1];
    COUNT(SELECT id FROM nodes 
    WHERE 
      location.coordinates[0] >= math::min([$x1, $x2]) AND
      location.coordinates[0] <= math::max([$x1, $x2]) AND
      location.coordinates[1] >= math::min([$y1, $y2]) AND
      location.coordinates[1] <= math::max([$y1, $y2]) AND 
      ${node1} != ${node2});
  `,

  knn: ({ node1 }, k) => `
    LET $source = (SELECT location FROM nodes:${node1})[0].location;
    SELECT id, geo::distance(location, $source) AS distance
    FROM nodes
    WHERE id != nodes:${node1}
    ORDER BY distance LIMIT ${k};
  `,
};

function sanitizeId(id) {
  return parseInt(id.split(':')[1]);
}

async function queryDistance() {
  console.time('Query All Distance');

  for (const pair of node_pairs) {
    let { result, time } = await client.query(queries.distance(pair));
    let filename = fileHandler.distanceFileName(pair);
    fileHandler.writeOut({
      queryName: dirQueries.distance,
      filename,
      data: { time, result },
    });
  }

  console.timeEnd('Query All Distance');
}

async function queryRadiusRange() {
  console.time('Query Radius Range');

  for (const pair of node_pairs) {
    for (const r of radius) {
      let { result, time } = await client.query(queries.radiusRange(pair, r));
      let filename = fileHandler.radiusRQFileName({
        node1: pair.node1,
        radius: r,
      });
      fileHandler.writeOut({
        queryName: dirQueries.radius,
        filename,
        data: { time, result: result.map(({ id }) => sanitizeId(id)) },
      });
    }
  }

  console.timeEnd('Query Radius Range');
}

async function queryWindowRange() {
  for (const pair of node_pairs) {
    let { result, time } = await client.query(queries.windowRange(pair));

    let filename = fileHandler.windowRQFileName(pair);
    fileHandler.writeOut({
      queryName: dirQueries.window,
      filename,
      data: { time, result: result.map(({ id }) => sanitizeId(id)) },
    });
  }
}

async function queryRangeCount() {
  console.time('Query All Range Count');
  for (const pair of node_pairs) {
    // window
    let { result, time } = await client.query(queries.windowRangeCount(pair));

    let filename = fileHandler.rangeCountFileName({
      ...pair,
      type: dirQueries.windowCount,
    });

    fileHandler.writeOut({
      queryName: dirQueries.windowCount,
      filename,
      data: { time, result },
    });

    for (const r of radius) {
      let { result, time } = await client.query(
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
        data: { time, result },
      });
    }
  }
  console.timeEnd('Query All Range Count');
}

async function queryKNN() {
  console.time('Query All KNN');

  for (const pair of node_pairs) {
    for (const k of ks) {
      let { time, result } = await client.query(queries.knn(pair, k));

      let filename = fileHandler.knnFileName({ ...pair, k });
      fileHandler.writeOut({
        queryName: dirQueries.knn,
        filename,
        data: { time, result: result.map(({ id }) => sanitizeId(id)) },
      });
    }
  }
  console.timeEnd('Query All KNN');
}

async function queryKClosestPair() {
  for (const k of ks) {
    break;
  }
}

export async function runAllSurrealdb() {
  console.log('Running SurrealDB queries');
  await client.connect();
  // await queryDistance();
  // await queryRadiusRange();
  // await queryWindowRange();
  // await queryRangeCount();
  // await queryKNN();
  await queryKClosestPair();
  await client.close();
}

await runAllSurrealdb();
