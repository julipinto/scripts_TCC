import MysqlConnection from '../connections/MysqllConnection.js';
import { node_pairs } from '../utils/params.js';

async function getLowerLeftTopRight(params) {
  const client = new MysqlConnection({
    database: 'map',
    user: 'root',
    password: 'root',
  });

  await client.connect();
  await client.query('SELECT NOW();');

  let ordered = [];
  let { min, max } = Math;
  let coords = [];

  for (let { node1, node2 } of params) {
    let { x1, y1 } = (
      await client.query(
        `SELECT ST_X(location) AS x1, ST_Y(location) AS y1 FROM nodes WHERE node_id = ${node1};`
      )
    ).result[0][0];

    let { x2, y2 } = (
      await client.query(
        `SELECT ST_X(location) AS x2, ST_Y(location) AS y2 FROM nodes WHERE node_id = ${node2};`
      )
    ).result[0][0];

    // ensuring any point in brasil will be calculated correctly (because brazil ultrapasses equator line)
    let projY1 = y1 - 5;
    let projY2 = y2 - 5;

    // let isN1LL = min(x1, x2) === x1 && max(projY1, projY2) === projY1;
    // let isN1TR = max(x1, x2) === x1 && min(projY1, projY2) === projY1;
    // let isN2LL = min(x1, x2) === x2 && max(projY1, projY2) === projY2;
    // let isN2TR = max(x1, x2) === x2 && min(projY1, projY2) === projY2;
    let isP1LL = min(x1, x2) === x1 && min(projY1, projY2) === projY1;
    let isP1TR = max(x1, x2) === x1 && max(projY1, projY2) === projY1;
    let isP2LL = min(x1, x2) === x2 && min(projY1, projY2) === projY2;
    let isP2TR = max(x1, x2) === x2 && max(projY1, projY2) === projY2;

    // console.log(isN1LL, isN1TR, isN2LL, isN2TR);

    if (isP1LL && isP2TR) {
      ordered.push({ node1, node2 });
      coords.push([
        [x1, y1],
        [x2, y2],
      ]);
    } else if (isP2LL && isP1TR) {
      ordered.push({ node1: node2, node2: node1 });
      coords.push([
        [x2, y2],
        [x1, y1],
      ]);
    } else {
      ordered.push('NOT LL TR');
    }
  }

  console.log(ordered);
  console.log(coords);

  await client.close();
}

await getLowerLeftTopRight(node_pairs);

// let s = new Set();
// for (let { node1, node2 } of node_pairs) {
//   s.add(node1), s.add(node2);
// }

// console.log(s.size);
