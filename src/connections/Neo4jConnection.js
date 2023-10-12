import neo4j from 'neo4j-driver';
import SpinerLog from '../utils/spinnerlog.js';
import { round } from '../utils/calc.js';

export default class Neo4jConnection {
  #client = null;

  constructor({ database, hostname, port, user, password }) {
    this.hostname = hostname ?? 'neo4j_container';
    this.port = port ?? 7687;
    this.database = database;
    this.user = user;
    this.password = password;

    this.log = new SpinerLog('neo4j');
  }

  async connect() {
    this.log.connectDB();

    if (!this.client) {
      try {
        this.#client = neo4j.driver(
          `neo4j://${this.hostname}:${this.port}/neo4j`,
          neo4j.auth.basic(this.user, this.password),
          {
            maxConnectionLifetime: this.connection_timeout,
            maxConnectionPoolSize: 50,
            database: this.database,
          }
        );

        await this.#client.getServerInfo();
        this.log.connectedDB();
      } catch (error) {
        this.log.errorConnectDB(error);
      }
    }
  }

  async query(sql, params = {}) {
    let start = performance.now();
    let session = this.#client.session();
    let result = await session.run(sql, { ...params });
    session.close();
    return {
      result,
      time: round(performance.now() - start),
    };
  }

  async close() {
    await this.#client.close();
  }
}
