import PostgresConnection from '../connections/PostgresConnection.js';
import { ks, node_pairs, radius, tagClosestPair } from '../utils/params.js';
import FileHandler, { dirQueries } from '../utils/FileHandler.js';

const fileHandler = new FileHandler('postgres');

const client = new PostgresConnection({
  database: 'map',
  user: 'root',
  password: 'root',
});

const queries = {
  distance: ({ node1, node2 }) => `SELECT
    n1.node_id AS source_node_id,
    n2.node_id AS target_node_id,
    ST_DistanceSphere(n1.location, n2.location) AS distance
    FROM nodes n1 JOIN
    nodes n2 ON n1.node_id = ${node1} AND n2.node_id = ${node2};`,

  radiusRange: ({ node1 }, radius) => `SELECT n.node_id, n.location
    FROM nodes n
    JOIN nodes central_node
    ON central_node.node_id = ${node1}
    WHERE ST_DWithin(n.location, central_node.location, ${radius}, true)`,

  windowRange: ({ node1, node2 }) => `SELECT n.node_id,  n.location
    FROM nodes n
    JOIN (SELECT
        (SELECT location FROM nodes WHERE node_id = ${node1}) AS p1,
        (SELECT location FROM nodes WHERE node_id = ${node2}) AS p2
    ) AS envelope ON
    ST_Contains(ST_MakeEnvelope(ST_X(envelope.p1), ST_Y(envelope.p1), ST_X(envelope.p2), ST_Y(envelope.p2), 4326)::geometry, n.location::geometry);`,

  radiusRangeCount: ({ node1 }, radius) => `SELECT COUNT(*) AS count
    FROM nodes n
    JOIN nodes central_node ON central_node.node_id = ${node1}
    WHERE ST_DWithin(n.location, central_node.location, ${radius}, true);`,

  windowRangeCount: ({ node1, node2 }) => `SELECT COUNT(*) AS count
    FROM nodes n
    JOIN (SELECT
        (SELECT location FROM nodes WHERE node_id = ${node1}) AS p1,
        (SELECT location FROM nodes WHERE node_id = ${node2}) AS p2
    ) AS envelope ON
    ST_Contains(ST_MakeEnvelope(ST_X(envelope.p1), ST_Y(envelope.p1), ST_X(envelope.p2), ST_Y(envelope.p2), 4326)::geometry, n.location::geometry);`,

  knn: ({ node1 }, k) => `SELECT
    target_node.node_id AS target_id,
    source_node.node_id AS neighbor_id,
    ST_Distance(target_node.location, source_node.location) AS distance
FROM
    nodes target_node,
    LATERAL (
        SELECT
            n.node_id,
            n.location
        FROM
            nodes n
        WHERE
            n.node_id != target_node.node_id
        ORDER BY
            target_node.location <-> n.location
        LIMIT
            ${k}
    ) AS source_node
WHERE
    target_node.node_id = ${node1};`,

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
};

async function queryDistance() {
  // let { result } = await client.query(queries.distance(node_pairs[0]));
  // let re = [];

  console.time('Query All Distance');
  for (let pair of node_pairs) {
    let { result, time } = await client.query(queries.distance(pair));
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
    // console.log(result.rows);
    // break;

    let hashIds = new Set();

    let filename = fileHandler.kClosestPairFileName({ k });

    let r = result.rows.filter(({ node_id1, node_id2 }) => {
      if (hashIds.has(node_id1) || hashIds.has(node_id2)) {
        return false;
      }
      hashIds.add([node_id1, node_id2]);
      return true;
    });

    fileHandler.writeOut({
      queryName: dirQueries.kClosestPair,
      filename,
      data: { time, result: r },
    });
  }
  console.timeEnd('Query All K Closest Pair');
}

export async function runAllPostgres() {
  console.log('Running Postgres queries');
  await client.connect();
  await client.query('SELECT NOW();');
  await queryDistance();
  await queryRadiusRange();
  await queryWindowRange();
  await queryRangeCount();
  await queryKNN();
  await queryKClosestPair();
  await client.close();
}

// await runAllPostgres();

// await queryDistance();
