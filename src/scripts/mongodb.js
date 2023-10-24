import MongodbConnection from '../connections/MongodbConnection.js';
import { ks, node_pairs, radius, tagClosestPair } from '../utils/params.js';
import FileHandler, { dirQueries } from '../utils/FileHandler.js';
import { removeDuplicates } from '../utils/removeKCPDuplucates.js';
import { round } from '../utils/calc.js';
import { districtsFeatures } from '../utils/districtsPolygonHandler.js';

const fileHandler = new FileHandler('mongodb');

const client = new MongodbConnection({
  database: 'map',
  user: 'root',
  password: 'root',
});

// First we need to fetch the nodes from the database
let fetchedPoints = [];

async function fetchNodes() {
  console.time('Pre-Fetch All Nodes');
  for (const pair of node_pairs) {
    const node1 = await client.nodes_collection.findOne({
      _id: pair.node1,
    });

    const node2 = await client.nodes_collection.findOne({
      _id: pair.node2,
    });

    fetchedPoints.push({ node1, node2 });
  }
  console.timeEnd('Pre-Fetch All Nodes');
}

function metresToRadians(metres) {
  return metres / 1000 / 6378.1;
}

async function queryDistance() {
  console.time('Query All Distance');

  for (const pair of fetchedPoints) {
    let start = performance.now();
    let pipeline = [
      {
        $geoNear: {
          near: {
            type: 'Point',
            coordinates: pair.node1.location.coordinates,
          },
          distanceField: 'distance',
          spherical: true,
          query: { _id: pair.node2._id }
        },
      },
    ];

    let aggregate = client.nodes_collection.aggregate(pipeline);

    let result = await aggregate.toArray();

    let filename = fileHandler.distanceFileName({
      node1: pair.node1._id,
      node2: pair.node2._id,
    });
    fileHandler.writeOut({
      queryName: dirQueries.distance,
      filename,
      data: {
        time: round(performance.now() - start),
        result: result[0].distance,
      },
    });
  }
  console.timeEnd('Query All Distance');
}
// let re = [];

// await queryDistance();

async function queryRadiusRange() {
  console.time('Query All Radius Range');

  // console.log(fetchedPoints[0].node1);
  for (const { node1 } of fetchedPoints) {
    for (const r of radius) {
      let start = performance.now();

      const result = await client.nodes_collection
        .find({
          $or: [{ amenity: { $exists: true } }, { shop: { $exists: true } }],
          location: {
            $geoWithin: {
              $centerSphere: [node1.location.coordinates, metresToRadians(r)],
            },
          },
        })
        .toArray();

      let filename = fileHandler.radiusRQFileName({
        node1: node1._id,
        radius: r,
      });

      fileHandler.writeOut({
        queryName: dirQueries.radius,
        filename,
        data: {
          time: round(performance.now() - start),
          result: result.map((node) => node._id),
        },
      });
    }
  }
  console.timeEnd('Query All Radius Range');
}

// await queryRadiusRange();

async function queryWindowRange() {
  console.time('Query All Window Range');

  for (const { node1, node2 } of fetchedPoints) {
    let start = performance.now();

    const result = await client.nodes_collection
      .find({
        $or: [{ amenity: { $exists: true } }, { shop: { $exists: true } }],
        location: {
          $geoWithin: {
            $box: [node1.location.coordinates, node2.location.coordinates],
          },
        },
      })
      .toArray();

    let filename = fileHandler.windowRQFileName({
      node1: node1._id,
      node2: node2._id,
    });

    fileHandler.writeOut({
      queryName: dirQueries.window,
      filename,
      data: {
        time: round(performance.now() - start),
        result: result.map((node) => node._id),
      },
    });
  }
  console.timeEnd('Query All Window Range');
}

// await queryWindowRange();

async function queryRangeCount() {
  console.time('Query All Range Count');

  for (const { node1, node2 } of fetchedPoints) {
    let startWindow = performance.now();
    let result = await client.nodes_collection.countDocuments({
      $or: [{ amenity: { $exists: true } }, { shop: { $exists: true } }],
      location: {
        $geoWithin: {
          $box: [node1.location.coordinates, node2.location.coordinates],
        },
      },
    });
    let filenameWindow = fileHandler.rangeCountFileName({
      node1: node1._id,
      node2: node2._id,
      type: dirQueries.windowCount,
    });

    fileHandler.writeOut({
      queryName: dirQueries.windowCount,
      filename: filenameWindow,
      data: {
        time: round(performance.now() - startWindow),
        result,
      },
    });

    for (const r of radius) {
      let startRadius = performance.now();
      const result = await client.nodes_collection
        .find({
          $or: [{ amenity: { $exists: true } }, { shop: { $exists: true } }],
          location: {
            $geoWithin: {
              $centerSphere: [node1.location.coordinates, metresToRadians(r)],
            },
          },
        })
        .toArray();

      let filenameRadius = fileHandler.rangeCountFileName({
        node1: node1._id,
        radius: r,
        type: dirQueries.radiusCount,
      });

      fileHandler.writeOut({
        queryName: dirQueries.radiusCount,
        filename: filenameRadius,
        data: {
          time: round(performance.now() - startRadius),
          result,
        },
      });
    }
  }
  console.timeEnd('Query All Range Count');
}

// await queryRangeCount();

async function queryKNN() {
  console.time('Query All KNN');

  for (const { node1 } of fetchedPoints) {
    for (const k of ks) {
      const start = performance.now();

      const pipeline = [
        {
          $geoNear: {
            near: {
              type: 'Point',
              coordinates: node1.location.coordinates,
            },
            distanceField: 'distance',
            spherical: true,
          },
        },
        { $match: { _id: { $ne: node1._id }, amenity: 'restaurant' } },
        { $limit: k },
      ];

      const result = await client.nodes_collection
        .aggregate(pipeline)
        .toArray();

      let filename = fileHandler.knnFileName({
        node1: node1._id,
        k,
      });

      fileHandler.writeOut({
        queryName: dirQueries.knn,
        filename,
        data: {
          time: round(performance.now() - start),
          result: result.map((node) => node._id),
        },
      });
    }
  }
  console.timeEnd('Query All KNN');
}

// await queryKNN();

async function queryKClosestPair() {
  console.time('Query All K Closest Pair');

  for (let k of ks) {
    const start = performance.now();

    const { key, value } = tagClosestPair;

    const result = await client.nodes_collection
      .aggregate([
        { $match: { [key]: value } },
        {
          $lookup: {
            from: 'nodes',
            as: 'closestNode',
            let: { coords: '$location.coordinates', prev_id: '$_id' },
            pipeline: [
              {
                $geoNear: {
                  near: {
                    type: 'Point',
                    coordinates: '$$coords',
                  },
                  distanceField: 'distFromMe',
                  spherical: true,
                  query: { [key]: value },
                },
              },
              { $match: { distFromMe: { $gt: 0 } } },
            ],
          },
        },
        { $unwind: '$closestNode' },
        { $sort: { 'closestNode.distFromMe': 1 } },
        { $limit: k * 2 },
      ])
      .toArray();

    let res = result.map((r) => ({
      node_id1: r._id,
      node_id2: r.closestNode._id,
      distance: r.closestNode.distFromMe,
    }));

    let withoutDuplicates = removeDuplicates(res);

    let filename = fileHandler.kClosestPairFileName({
      k,
    });

    fileHandler.writeOut({
      queryName: dirQueries.kClosestPair,
      filename,
      data: {
        time: round(performance.now() - start),
        result: withoutDuplicates,
      },
    });
  }
  console.timeEnd('Query All K Closest Pair');
}

async function querySpatialJoin() {
  console.time('Query All Spatial Join');
  for (const district_feature of districtsFeatures) {
    const start = performance.now();

    const result = await client.nodes_collection
      .find({
        $or: [{ amenity: { $exists: true } }, { shop: { $exists: true } }],
        location: {
          $geoWithin: { $geometry: district_feature.geometry },
        },
      })
      .toArray();

    // let { time, result } = await client.query(query);

    let filename = fileHandler.spatialJoinFileName({
      district: district_feature.properties.district,
    });

    fileHandler.writeOut({
      queryName: dirQueries.spatialJoin,
      filename,

      data: {
        time: round(performance.now() - start),
        result: result.map((node) => node._id),
      },
    });
  }
  console.timeEnd('Query All Spatial Join');
}

export async function runAllMongodb() {
  console.log('Running MongoDB queries');
  await client.connect();
  await fetchNodes();

  await queryDistance();
  await queryRadiusRange();
  await queryWindowRange();
  await queryRangeCount();
  await queryKNN();
  await queryKClosestPair();
  await querySpatialJoin();
  await client.close();
}