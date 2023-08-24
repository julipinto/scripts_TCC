import mysql from 'mysql2/promise';
import SpinerLog from '../utils/spinnerlog.js';

export default class MysqlConnection {
  #client = null;

  constructor({ database, hostname, port, user, password }) {
    this.hostname = hostname ?? 'localhost';
    this.port = port ?? 3306;
    this.database = database;
    this.user = user;
    this.password = password;

    this.log = new SpinerLog('mysql');
  }

  async connect() {
    this.log.connectDB();

    if (!this.client) {
      let config = {
        host: this.hostname,
        port: this.port,
        database: this.database,
        user: this.user,
        password: this.password,
      };

      try {
        this.#client = await mysql.createPool(config);
        this.log.connectedDB();
      } catch (error) {
        this.log.errorConnectDB(error);
      }
    }
  }

  async query(sql) {
    let start = performance.now();
    let result = await this.#client.query(sql);
    // console.log(result);
    return {
      result,
      time: performance.now() - start,
    };
  }

  async close() {
    await this.#client.end();
  }
}
