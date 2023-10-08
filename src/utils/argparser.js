import { parseArgs } from 'node:util';
import { exit } from 'node:process';

const options = {
  dbmanager: {
    type: 'string',
    short: 'm',
  },
  help: {
    type: 'boolean',
  },
};

const { values } = parseArgs({ options });

let requiredArgsMissing = [];
['dbmanager'].forEach((key) => {
  if (!values[key]) {
    requiredArgsMissing.push(key);
  }
});

if (values.help || Object.keys(values).length === 0) {
  console.log(`
Welcome to PBF Parser!

Usage:
Required arguments:
  -m, --dbmanager <manager>          Database manager. Valid values: mysql | postgres | neo4j | mongodb | marklogic | surrealdb (default: mysql).

Optional arguments:
  -h, --help                         Show this help message and exit.
`);
  exit(0);
}

if (requiredArgsMissing.length > 0) {
  throw new Error(
    `Missing required arguments: ${requiredArgsMissing.join(', ')}`
  );
}

const args = values;

export { args };
