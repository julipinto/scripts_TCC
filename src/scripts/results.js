import { readdir } from 'fs/promises';
import { runAllMySQL } from './mysql.js';
import { runAllPostgres } from './postgres.js';
import { runAllMongodb } from './mongodb.js';
import { runAllNeo4j } from './neo4j.js';

async function runAll() {
  await runAllMySQL();
  console.log('');
  await runAllPostgres();
  console.log('');
  await runAllMongodb();
  console.log('');
  await runAllNeo4j();
}

await runAll();

async function readDirs() {
  return await readdir('./out');
}

// let dirs = await readDirs();
// console.log(dirs);
