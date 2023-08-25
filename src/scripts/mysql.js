import { join } from 'path';
import MysqlConnection from '../connections/MysqllConnection.js';
import { ks, node_pairs, radius } from '../utils/params.js';
import { writeFile } from 'fs/promises';
// r = {
//   time: 0,
//   result: {
//     rows: [],
//     ...
//   }

const client = new MysqlConnection({
  database: 'map',
  user: 'root',
  password: 'root',
});

const FORMAT = 'json';

const queries = {
  distance: {
    out: ({ node1, node2 }) =>
      join(
        'src',
        'out',
        'mysql',
        'distance',
        `distance_${node1}_${node2}.${FORMAT}`
      ),
    query: ({ node1, node2 }) =>
      'SELECT ' +
      'ST_Distance(' +
      `(SELECT location FROM nodes WHERE node_id = ${node1}),` +
      `(SELECT location FROM nodes WHERE node_id = ${node2})` +
      ') AS distance;',
  },
  rrq: {
    out: ({ node1 }, radius) =>
      join(
        'src',
        'out',
        'mysql',
        'distance',
        `rrq_${node1}_${radius}.${FORMAT}`
      ),
    query: ({ node1 }, radius) =>
      'SELECT node_id, location FROM nodes ' +
      'WHERE ST_Distance_Sphere(location, ' +
      `(SELECT location FROM nodes WHERE node_id = ${node1})) <= ${radius};`,
  },
};

await client.connect();
await client.query('SELECT NOW();');

// let {result} = await client.query('SELECT * FROM nodes ORDER BY RAND() LIMIT 40;');

// distance
let times = [];
let results = [];
for (let pair of node_pairs) {
  let { result, time } = await client.query(queries.distance.query(pair));
  let r = result[0][0].distance;
  await writeFile(
    queries.distance.out(pair),
    JSON.stringify({ result: r, time })
  );
}
// console.table({ mysql: times });
// console.table({ mysql: results });

// rrq
// const times = radius.reduce((acc, r) => {
//   return { ...acc, [r]: [] };
// }, {});

// const results = radius.reduce((acc, r) => {
//   return { ...acc, [r]: [] };
// }, {});

// let { result } = await client.query(queries.rrq(node_pairs[0], 50));
// console.log(result[0].map(({ node_id }) => node_id));

// for (let pair of node_pairs) {
//   for (let r of radius) {
//     let { time, result } = await client.query(queries.rrq(pair, r));
//     times[r].push(round(time));
//     results[r].push(result[0].map(({ node_id }) => node_id));
//   }
// }

// for (let r of radius) {
//   console.log('timers ', r);
//   console.table({ mysql: times[r] });
//   console.log('results ', r);
//   console.table({ mysql: results[r] });
// }

// console.table({ mysql: times[radius[0]] });
// console.table({ mysql: times[radius[1]] });
// console.table({ mysql: times[radius[2]] });
// console.table({ mysql: times[radius[3]] });

// console.table({ mysql: results });

client.close();
