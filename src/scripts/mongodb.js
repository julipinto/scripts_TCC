import MongodbConnection from '../connections/MongodbConnection.js';
import { ks, node_pairs, radius } from '../utils/params.js';
import FileHandler, { dirQueries } from '../utils/fileHandler.js';
import { ObjectId } from 'mongodb';

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

async function queryDistance() {
  let re = [];

  for (const pair of fetchedPoints) {
    let pipeline = [
      {
        $geoNear: {
          near: {
            type: 'Point',
            coordinates: pair.node1.location.coordinates,
          },
          distanceField: 'distance',
          spherical: true,
          // includeLocs: 'locationa',
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
    re.push(result[0].distance);
  }

  console.log(re.join('\n'));
}
// let re = [];

await queryDistance();

async function knn() {
  const node1 = await client.nodes_collection.findOne({
    _id: node_pairs[0].node1,
  });

  let result = client.nodes_collection.aggregate([
    {
      $nearSphere: {
        near: {
          type: 'Point',
          coordinates: [-118.2437, 34.0522],
        },
        distanceField: 'distance',
        spherical: true,
      },
    },
    {
      $match: {
        _id: 123456, // Exclui o próprio ponto
      },
    },
  ]);

  console.log(JSON.stringify(await result.toArray()));
}

// await knn();

// let pipeline = [
//   {
//     $geoNear: {
//       near: {
//         type: 'Point',
//         coordinates: node1.location.coordinates,
//       },
//       distanceField: 'distance',
//       spherical: true,
//     },
//   },
//   {
//     $limit: ks[0],
//   },
// ];

// let pipeline = [
//   {
//     $geoNear: {
//       near: {
//         type: 'Point',
//         coordinates: node1.location.coordinates,
//       },
//       distanceField: 'distance',
//       spherical: true,
//     },
//   },
//   {
//     $match: {
//       _id: { $not: { $eq: node1._id } }, // Exclui o próprio ponto
//     },
//   },
//   {
//     $limit: 6,
//   },
// ];

// let pipeline = [
//   {
//     $geoNear: {
//       near: {
//         type: 'Point',
//         coordinates: node1.location.coordinates,
//       },
//       distanceField: 'distance',
//       spherical: true,
//     },
//   },
//   {
//     $match: {
//       _id: { $not: { $eq: node1._id } }, // Exclui o próprio ponto
//     },
//   },
//   {
//     $sort: { distance: 1 }, // Ordena os resultados pela distância em ordem crescente
//   },
//   {
//     $limit: ks[0],
//   },
// ];

// let result = client.nodes_collection.aggregate(pipeline);

// let result = client.nodes_collection
//   .find({
//     location: {
//       $nearSphere: {
//         $geometry: { type: 'Point', coordinates: node1.location.coordinates },
//       },
//     },
//   })
//   .limit(ks[1] + 1);

// client.nodes_collection.insertOne({
//   _id: 123456,
//   location: { type: 'Point', coordinates: [-74.006, 40.7128] },
// });

// SELECT ST_Distance_Sphere(
//     ST_GeomFromText('POINT(-38.5023 -12.9716)', 4326),
//     ST_GeomFromText('POINT(-46.6333 -23.5505)', 4326)
// ) AS distance;

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
