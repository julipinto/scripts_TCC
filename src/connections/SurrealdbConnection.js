import SurrealDB from 'surrealdb.js';
import SpinerLog from '../utils/spinnerlog.js';
import { round } from '../utils/calc.js';

export default class MysqlConnection {
  #client = new SurrealDB();

  constructor({ database, hostname, port, user, password }) {
    this.hostname = hostname ?? 'localhost';
    this.port = port ?? 8000;
    this.database = database;
    this.user = user;
    this.password = password;

    this.log = new SpinerLog('surrealdb');
  }

  async connect() {
    try {
      this.log.connectDB();
      const url = `http://${this.hostname}:${this.port}/rpc`;
      const config = { auth: { user: this.user, pass: this.password } };
      await this.#client.connect(url, config);
      await this.#client.use({ ns: 'root', db: 'map' });
      this.log.connectedDB();
    } catch (error) {
      this.log.errorConnectDB(error);
    }
  }

  async query(query, vars) {
    let start = performance.now();
    let r = await this.#client.query(query, vars);

    return {
      result: r[r.length - 1].result,
      // result: r,
      time: round(performance.now() - start),
    };
  }

  async close() {
    this.#client.close();
  }
}
