import MongodbConnection from '../connections/MongodbConnection.js';
import { ks, node_pairs, radius } from '../utils/params.js';
import FileHandler, { dirQueries } from '../utils/fileHandler.js';
import { round } from '../utils/calc.js';

const fileHandler = new FileHandler('mongodb');

const client = new MongodbConnection({
  database: 'map',
  user: 'root',
  password: 'root',
});

await client.connect();

const queries = {
  distance: ({ node1: sourceNodeId, node2: targetNodeId }) => {
    return {};
  },

  radiusRange: ({ node1 }, radius) => ``,
  windowRange: ({ node1, node2 }) => ``,
  radiusRangeCount: ({ node1 }, radius) => ``,
  windowRangeCount: ({ node1, node2 }) => ``,
  knn: ({ node1 }, k) => ``,
  shortestPath: ({ node1: source_node_id, node2: target_node_id }) => ``,
};

// First we need to fetch the nodes from the database
let fetchedPoints = [];
for (const pair of node_pairs) {
  const node1 = await client.nodes_collection.findOne({
    _id: pair.node1,
  });

  const node2 = await client.nodes_collection.findOne({
    _id: pair.node2,
  });

  fetchedPoints.push({ node1, node2 });
}

function metresToRadians(metres) {
  return metres / 1000 / 6378.1;
}

async function queryDistance() {
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
        },
      },
      {
        $match: {
          _id: pair.node2._id,
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
}
// let re = [];

// await queryDistance();

async function queryRadiusRange() {
  // console.log(fetchedPoints[0].node1);
  for (const { node1 } of fetchedPoints) {
    for (const r of radius) {
      let start = performance.now();

      const result = await client.nodes_collection
        .find({
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
}

// await queryRadiusRange();

async function queryWindowRange() {
  for (const { node1, node2 } of fetchedPoints) {
    let start = performance.now();

    const result = await client.nodes_collection
      .find({
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
}

// await queryWindowRange();

async function queryRangeCount() {
  for (const { node1, node2 } of fetchedPoints) {
    let startWindow = performance.now();
    let result = await client.nodes_collection.countDocuments({
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
      let result = await client.nodes_collection.countDocuments({
        location: {
          $geoWithin: {
            $centerSphere: [node1.location.coordinates, metresToRadians(r)],
          },
        },
      });

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
}

// await queryRangeCount();

async function queryKNN() {
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
        {
          $match: {
            _id: { $ne: node1._id }, // Exclude node1
          },
        },
        {
          $limit: 5,
        },
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
}

async function queryClosestPair() {
  const result = await client.nodes_collection
    .aggregate([
      { $match: { power: 'tower' } },
      {
        $project: {
          _id: 0,
          node1: { $mergeObjects: '$$ROOT' },
        },
      },
      {
        $lookup: {
          from: 'nodes',
          as: 'node2',
          let: {
            cur_id: '$node1._id',
            coords: '$node1.location.coordinates',
          },
          pipeline: [
            {
              $geoNear: {
                near: {
                  type: 'Point',
                  coordinates: '$$coords',
                },
                distanceField: 'distance',
                spherical: true,
              },
            },
            { $match: { power: 'tower', distance: { $gt: 0 } } },
            { $sort: { distance: 1 } },
            { $limit: 1 },
          ],
        },
      },
      { $unwind: '$node2' },
      { $sort: { 'node2.distance': 1 } },
      { $limit: 1 },
      { $set: { distance: '$node2.distance' } },
      { $unset: 'node2.distance' },
    ])
    .toArray();

  console.log(result);

  console.log(
    result.filter((node) => node.node1._id === node.node2._id).length
  );
}
// await queryKNN();
await queryClosestPair();

await client.close();

// async function teste() {
//   // await client.nodes_collection.insertOne({
//   //   _id: 0,
//   //   location: { type: 'Point', coordinates: [0, 0] },
//   // });

//   let result = client.nodes_collection.aggregate([
//     {
//       $geoNear: {
//         //result is half the Earth's circumference, distance from (0,0) to (180,0)
//         near: {
//           type: 'Point',
//           coordinates: [180, 0],
//         },
//         distanceField: 'distance',
//         distanceMultiplier: 1 / Math.PI, // 2*Pi*radius is the circumference, so radius is half the circumference divided by Pi
//         spherical: true,
//       },
//     },
//     {
//       $match: {
//         _id: 0, // Filter only the point of interest
//       },
//     },
//   ]);

//   console.log(JSON.stringify(await result.toArray()));
// }
// await teste();

// { $match: { power: 'tower' } },
// {
//   $set: {
//     lat: {
//       $multiply: [{ $last: '$location.coordinates' }, 0.017452778],
//     },
//     long: {
//       $multiply: [{ $first: '$location.coordinates' }, 0.017452778],
//     },
//   },
// },
//     ],
//   },
// },
// {
//   $unwind: '$closestNode',
// },
// {
//   $match: {
//     'closestNode._id': { $ne: '$_id' },
//   },
// },
// {
//   $addFields: {
//     distance: {
//       $sqrt: {
//         $sum: [
//           {
//             $pow: [
//               {
//                 $subtract: [
//                   '$location.coordinates[0]',
//                   '$closestNode.location.coordinates[0]',
//                 ],
//               },
//               2,
//             ],
//           },
//           {
//             $pow: [
//               {
//                 $subtract: [
//                   '$location.coordinates[1]',
//                   '$closestNode.location.coordinates[1]',
//                 ],
//               },
//               2,
//             ],
//           },
//         ],
//       },
//     },
//   },
// },
// { $match: { distance: { $gt: 0 } } },
// {
//   $sort: {
//     distance: 1,
//   },
// },
// {
//   $limit: 1,
// },
