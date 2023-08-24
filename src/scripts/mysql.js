import MysqlConnection from '../connections/MysqllConnection.js';

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

await client.connect();

let r = await client.query('SELECT * FROM nodes ORDER BY RAND() LIMIT 40;');
console.log(r.time);
console.log(r.result);

client.close();
