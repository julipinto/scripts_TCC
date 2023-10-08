import { join, resolve } from 'path';
import { createWriteStream, existsSync, mkdirSync } from 'fs';
import { readFile } from 'fs/promises';

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

  writeOut({ queryName, filename, data }) {
    let dir = this.#createDirIfNotExists(
      resolve(join('out', this.database, queryName))
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
    if (type === dirQueries.windowCount) {
      return `${dirQueries.windowCount}_${node1}_${node2}.${this.FORMAT}`;
    }

    if (type === dirQueries.radiusCount)
      return `${dirQueries.radiusCount}_${node1}_r${radius}.${this.FORMAT}`;
  }

  knnFileName({ node1, k }) {
    return `knn_${node1}_k${k}.${this.FORMAT}`;
  }

  kClosestPairFileName({ k }) {
    return `kcp_k${k}.${this.FORMAT}`;
  }

  spatialJoinFileName({ district }) {
    return `sj_${district}.${this.FORMAT}`;
  }

  readIn({ filename, queryName }) {
    return readFile(
      resolve(join('out', this.database, queryName, filename)),
      'utf8'
    )
      .then((data) => {
        return JSON.parse(data);
      })
      .catch((err) => {
        if (err.errno == -4058) return undefined;
        throw err;
      });
  }
}

export const dirQueries = {
  distance: 'distance',
  knn: 'knn',
  radius: 'radiusRQ',
  window: 'windowRQ',
  radiusCount: 'radiusRQCount',
  windowCount: 'windowRQCount',
  kClosestPair: 'kclosestpair',
  spatialJoin: 'spatialjoin',
};
