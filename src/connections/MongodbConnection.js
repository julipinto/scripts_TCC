import { MongoClient } from 'mongodb';
import SpinerLog from '../utils/spinnerlog.js';
import { exit } from 'process';

export default class MongodbConnection {
  #client = null;
  nodes_collection = null;
  ways_collection = null;
  relations_collection = null;

  constructor({ database, hostname, port, user, password }) {
    this.hostname = hostname ?? 'localhost';
    this.port = port ?? 27017;
    this.database = database;
    this.user = user;
    this.password = password;

    this.url = `mongodb://${user}:${password}@${this.hostname}:${this.port}/${database}?authSource=admin`;
  }

  async connect() {
    this.log = new SpinerLog('mongodb');
    this.log.connectDB();
    this.#client = await MongoClient.connect(this.url, {
      useNewUrlParser: true,
    });

    try {
      await this.#client.connect();
      this.log.connectedDB();
    } catch (error) {
      this.log.errorConnectDB(error);
    }

    this.db = this.#client.db(this.database);
    this.nodes_collection = this.db.collection('nodes');

    // console.log(this.nodes_collection);

    // exit(0);
    this.nodes_collection = this.db.collection('nodes');
    this.ways_collection = this.db.collection('ways');
    this.relations_collection = this.db.collection('relations');

    return this.db;
  }

  async close() {
    await this.#client.close();
  }
}
