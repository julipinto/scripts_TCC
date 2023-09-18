import Neo4jConnection from '../connections/Neo4jConnection.js';
import { node_pairs } from '../utils/params.js';

async function getLowerLeftTopRight(params) {
  const client = new Neo4jConnection({
    database: 'neo4j',
    user: 'neo4j',
    password: 'root1234',
  });

  await client.connect();

  let winRQ = (n1, n2) =>
    'MATCH (node:POINT) ' +
    `MATCH (b1:POINT {id: ${n1}}) ` +
    `MATCH (b2:POINT {id: ${n2}}) ` +
    'WHERE point.withinBBox(node.location, b1.location, b2.location)' +
    'RETURN count(node) AS count;';

  let ordered = [];

  for (let { node1, node2 } of params) {
    let n1 = (await client.query(winRQ(node1, node2))).result.records
      .map((r) => r.get('count'))[0]
      .toNumber();

    let n2 = (await client.query(winRQ(node2, node1))).result.records
      .map((r) => r.get('count'))[0]
      .toNumber();

    if (n1 > n2) ordered.push({ node1, node2 });
    else if (n1 < n2) ordered.push({ node1: node2, node2: node1 });
    else ordered.push('Lower Left and Top Right was not identified');
  }

  console.log(ordered);
  await client.close();
  return ordered;
}

await getLowerLeftTopRight(node_pairs);
