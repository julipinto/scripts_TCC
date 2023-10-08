import { runAllMySQL } from './scripts/mysql.js';
import { runAllPostgres } from './scripts/postgres.js';
import { runAllMongodb } from './scripts/mongodb.js';
import { runAllNeo4j } from './scripts/neo4j.js';
import { runAllSurrealdb } from './scripts/surrealdb.js';

import { args } from './utils/argparser.js';

async function run() {
  switch (args.dbmanager) {
    case 'mysql':
      return await runAllMySQL();
    case 'postgres':
      return await runAllPostgres();
    case 'neo4j':
      return await runAllNeo4j();
    case 'mongodb':
      return await runAllMongodb();
    case 'surrealdb':
      return await runAllSurrealdb();
    case 'marklogic':
      throw new Error("I'm sorry, MarkLogic is not good to go yet");
    default:
      console.error('Unsuported dbmanager');
  }
}

await run();
