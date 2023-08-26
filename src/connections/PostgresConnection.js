import postgres from 'pg';
import SpinerLog from '../utils/spinnerlog.js';
import { round } from '../utils/calc.js';

export default class PostgresConnection {
  #client = null;

  constructor({ database, hostname, port, user, password }) {
    this.hostname = hostname ?? 'localhost';
    this.port = port ?? 5432;
    this.database = database;
    this.user = user;
    this.password = password;

    this.log = new SpinerLog('postgres');
  }

  async connect() {
    this.log.connectDB();

    if (!this.client) {
      this.#client = new postgres.Pool({
        host: this.hostname,
        port: this.port,
        database: this.database,
        user: this.user,
        password: this.password,
      });

      try {
        await this.#client.query('SELECT NOW()');
        this.log.connectedDB();
      } catch (error) {
        this.log.errorConnectDB(error);
      }
    }
  }

  async query(sql) {
    let start = performance.now();
    let result = await this.#client.query(sql);
    return {
      result,
      time: round(performance.now() - start),
    };
  }

  async close() {
    await this.#client.end();
  }
}
