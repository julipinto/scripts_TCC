import { join, resolve } from 'path';
import { createWriteStream, existsSync, mkdirSync } from 'fs';

export default class FileHandler {
  constructor(database) {
    this.database = database;
    this.FORMAT = 'json';
  }

  #createDirIfNotExists(path) {
    if (!existsSync(path)) {
      mkdirSync(path, { recursive: true });
    }
    return path;
  }

  writeOut({ query, filename, data }) {
    let dir = this.#createDirIfNotExists(
      resolve(join('out', this.database, query))
    );
    let path = resolve(join(dir, filename));
    let writeStream = createWriteStream(path);
    writeStream.write(JSON.stringify(data));
    writeStream.end();
  }

  distanceFileName({ node1, node2 }) {
    return `distance_${node1}_${node2}.${this.FORMAT}`;
  }

  radiusRQFileName({ node1, radius }) {
    return `radiusRQ_${node1}_r${radius}.${this.FORMAT}`;
  }

  windowRQFileName({ node1, node2 }) {
    return `windowRQ_${node1}_${node2}.${this.FORMAT}`;
  }

  rangeCountFileName({ node1, node2, type, radius }) {
    if (type === 'wrqCount') {
      return `windowRQCount_${node1}_${node2}.${this.FORMAT}`;
    }
    return `radiusRQCount_${node1}_r${radius}.${this.FORMAT}`;
  }

  knnFileName({ node1, k }) {
    return `knn_${node1}_k${k}.${this.FORMAT}`;
  }
}
