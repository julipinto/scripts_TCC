import MongodbConnection from '../connections/MongodbConnection.js';
import { dirQueries } from '../utils/FileHandler.js';

import { createWriteStream, existsSync, mkdirSync } from 'fs';
import {
  getWindowResults,
  getRadiusResults,
  getKNNResults,
  getKClosestPairs,
} from './results.js';

import drawCircle from 'circle-to-polygon';

const randomColors = [
  '#FF5733',
  '#7D3C98',
  '#3498DB',
  '#E74C3C',
  '#2ECC71',
  '#F1C40F',
  '#34495E',
  '#D35400',
  '#8E44AD',
  '#1ABC9C',
  '#FFA07A',
  '#6A1B9A',
  '#1E8449',
  '#E74C3C',
  '#F39C12',
  '#3498DB',
  '#D35400',
  '#27AE60',
  '#B9770E',
  '#9B59B6',
];

async function mapFeatures({ ids, highlight = [] }) {
  let objects = await client.nodes_collection
    .find({ _id: { $in: ids.map((r) => parseInt(r)) } })
    .toArray();

  return objects.map(({ _id, location, ...rest }) => ({
    type: 'Feature',
    geometry: { ...location },
    properties: {
      ...rest,
      ...(highlight.includes(_id) ? { 'marker-color': '#F5D723' } : {}),
    },
    id: _id,
  }));
}

const root = 'geoJSON';

const client = new MongodbConnection({
  database: 'map',
  user: 'root',
  password: 'root',
});

async function writeGeoJSON({ dir, file, geoJSON }) {
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  let writeStream = createWriteStream(file);
  await writeStream.write(JSON.stringify(geoJSON));
  await writeStream.end();
}

async function getWindowResultsGeoJSON() {
  let { results: rwin } = await getWindowResults();
  for (let [key, results] of Object.entries(rwin)) {
    let [_, pid1, pid2] = key.split('_');

    let {
      location: { coordinates: p1 },
    } = await client.nodes_collection.findOne({ _id: parseInt(pid1) });
    let {
      location: { coordinates: p2 },
    } = await client.nodes_collection.findOne({ _id: parseInt(pid2) });

    for (let [database, result] of Object.entries(results)) {
      let features = await mapFeatures({
        ids: result,
        highlight: [parseInt(pid1), parseInt(pid2)],
      });

      let geoJSON = {
        type: 'FeatureCollection',
        bbox: [p1[0], p1[1], p2[0], p2[1]],
        features: [
          ...features,
          {
            type: 'Feature',
            geometry: {
              type: 'Polygon',
              coordinates: [[p1, [p1[0], p2[1]], p2, [p2[0], p1[1]], p1]],
            },
            properties: { id: 'window' },
          },
        ],
      };

      let dir = `${root}/${dirQueries.window}/${key}`;
      let file = `${dir}/${database}_${key}`;
      await writeGeoJSON({ dir, file, geoJSON });
    }
  }
}

async function allRadiusResultsGeoJSON() {
  let { results: rrad } = await getRadiusResults();
  console.log(rrad);
  for (let [key, key_results] of Object.entries(rrad)) {
    let {
      location: { coordinates: p },
    } = await client.nodes_collection.findOne({ _id: parseInt(key) });

    for (let [r, databases] of Object.entries(key_results)) {
      for (let [database, result] of Object.entries(databases)) {
        let features = await mapFeatures({ ids: result, highlight: [key] });
        let circle = drawCircle(p, parseInt(r.replace('r', '')), 32);

        let geoJSON = {
          type: 'FeatureCollection',
          features: [
            ...features,
            {
              type: 'Feature',
              geometry: {
                type: 'Polygon',
                coordinates: circle.coordinates,
              },
              properties: { id: 'radius' },
            },
          ],
        };

        let dir = `${root}/${dirQueries.radius}/${key}/${r}`;
        let file = `${dir}/${database}_${key}`;
        await writeGeoJSON({ dir, file, geoJSON });
      }
    }
  }
}

async function allKNNResultsGeoJSON() {
  let { results: rKNN } = await getKNNResults();
  // console.log(rKNN);
  for (let [key, results] of Object.entries(rKNN)) {
    let {
      location: { coordinates: p },
    } = await client.nodes_collection.findOne({ _id: parseInt(key) });

    for (let [k, databases] of Object.entries(results)) {
      for (let [database, result] of Object.entries(databases)) {
        let features = await mapFeatures({ ids: result, highlight: [key] });

        let geoJSON = {
          type: 'FeatureCollection',
          features: [
            ...features,
            {
              type: 'Feature',
              geometry: {
                type: 'Point',
                coordinates: p,
              },
              properties: { id: 'source', 'marker-color': '#ff0000' },
            },
          ],
        };

        let dir = `${root}/${dirQueries.knn}/${key}/${k}`;
        let file = `${dir}/${database}_${key}`;
        await writeGeoJSON({ dir, file, geoJSON });
      }
    }
  }
}

async function allKClosestPairsGeoJSON() {
  let { results: rKCP } = await getKClosestPairs();

  for (let [key, results] of Object.entries(rKCP)) {
    for (let [database, result] of Object.entries(results)) {
      let features = [];

      for (let [index, { node_id1, node_id2 }] of Object.entries(result)) {
        let {
          location: { coordinates: p1 },
        } = await client.nodes_collection.findOne({ _id: parseInt(node_id1) });

        let {
          location: { coordinates: p2 },
        } = await client.nodes_collection.findOne({ _id: parseInt(node_id2) });

        features.push({
          type: 'Feature',
          geometry: {
            type: 'Point',
            coordinates: p1,
          },
          properties: { 'marker-color': randomColors[index] },
          id: node_id1,
        });

        features.push({
          type: 'Feature',
          geometry: {
            type: 'Point',
            coordinates: p2,
          },
          properties: { 'marker-color': randomColors[index] },
          id: node_id2,
        });

        features.push({
          type: 'Feature',
          geometry: {
            type: 'LineString',
            coordinates: [p1, p2],
          },
          properties: { stroke: randomColors[index] },
          id: `${node_id1}_${node_id2}`,
        });
      }

      let geoJSON = {
        type: 'FeatureCollection',
        features,
      };
      let dir = `${root}/${dirQueries.kClosestPair}/${key}`;
      let file = `${dir}/${database}_${key}`;
      await writeGeoJSON({ dir, file, geoJSON });
    }
  }
}

async function allResultsGeoJSON() {
  await client.connect();
  // await getWindowResultsGeoJSON();
  // await allRadiusResultsGeoJSON();
  // await allKNNResultsGeoJSON();
  // await allKClosestPairsGeoJSON();

  await client.close();
}

await allResultsGeoJSON();
