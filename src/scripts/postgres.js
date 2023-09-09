import PostgresConnection from '../connections/PostgresConnection.js';
import { ks, node_pairs, radius } from '../utils/params.js';
import FileHandler, { dirQueries } from '../utils/fileHandler.js';

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
    target_node.node_id = ${node1};
`,
};

client.connect();
client.query('SELECT NOW();');

async function queryDistance() {
  // let { result } = await client.query(queries.distance(node_pairs[0]));
  // let re = [];

  for (let pair of node_pairs) {
    let { result, time } = await client.query(queries.distance(pair));
    // re.push(result.rows[0].distance);
    fileHandler.writeOut({
      queryName: dirQueries.distance,
      filename: fileHandler.distanceFileName(pair),
      data: { time, result: result.rows[0].distance },
    });
  }

  // console.log(re.join('\n'));
}

await queryDistance();

async function queryRadiusRange() {
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
}

async function queryWindowRange() {
  for (let pair of node_pairs) {
    let { result, time } = await client.query(queries.windowRange(pair));
    fileHandler.writeOut({
      queryName: dirQueries.window,
      filename: fileHandler.windowRQFileName(pair),
      data: { time, result: result.rows.map(({ node_id }) => node_id) },
    });
  }
}

async function queryRangeCount() {
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
}

// await queryRangeCount();

async function knn() {
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
}

// await knn();

async function runAll() {
  await queryDistance();
  await queryRadiusRange();
  await queryWindowRange();
  await queryRangeCount();
  await knn();
}

// await runAll();

// await queryDistance();

await client.close();
