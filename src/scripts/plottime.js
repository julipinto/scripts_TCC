import {
  getDistanceResults,
  getRadiusResults,
  getWindowResults,
  getRangeCountResults,
  getKClosestPairs,
  getKNNResults,
  getSpatialJoin,
} from './results.js';

const sanitizeDatabaseKey = {
  mysql: 'MySQL',
  postgres: 'PostgreSQL',
  mongodb: 'MongoDB',
  neo4j: 'Neo4j',
  surrealdb: 'SurrealDB',
};

function arrangeNoVariant(timestemps) {
  let data = {};
  for (let [_, all] of Object.entries(timestemps)) {
    for (let [database, time] of Object.entries(all)) {
      let key = sanitizeDatabaseKey[database];
      if (!(key in data)) data[key] = [];
      data[key].push(time);
    }
  }
  return data;
}

function arrangeVariant(timestemps) {
  let map = {};

  for (let [_, all] of Object.entries(timestemps)) {
    for (let [r, result] of Object.entries(all)) {
      for (let [database, time] of Object.entries(result)) {
        if (!(r in map)) map[r] = {};
        if (!(database in map[r])) map[r][database] = [];
        map[r][database].push(time);
      }
    }
  }

  return map;
}

// 1 variante
async function getDistancePlot() {
  const { timestemps } = await getDistanceResults();
  let data = arrangeNoVariant(timestemps);
  // console.log(data);
  return data;
}

// await getDistancePlot();

// 4 variantes ok
async function getRadiusPlot() {
  const { timestemps } = await getRadiusResults();
  let map = arrangeVariant(timestemps);
  return map;
}

// await getRadiusPlot();

// 1 variante
async function getWindowPlot() {
  const { timestemps } = await getWindowResults();
  let data = arrangeNoVariant(timestemps);
  return data;
}

// 1 variante + 4 variante ok
async function getRangeCountPlot() {
  const { timestempsRadius, timestempsWindow } = await getRangeCountResults();
  let mapWindow = arrangeNoVariant(timestempsWindow);
  let mapRadius = arrangeVariant(timestempsRadius);
  return { mapWindow, mapRadius };
}

// await getRangeCountPlot();

// 4 variantes ok
async function getKNNPlot() {
  const { timestemps } = await getKNNResults();
  let map = arrangeVariant(timestemps);
  return map;
  // let data = arrangeNoVariant(timestemps);
}

// 1 variante
async function getKClosestPairsPlot() {
  const { timestemps } = await getKClosestPairs();
  // console.log(timestemps);
  //  TEM QUE VER COMO É QUE VAI FAZER COM ESSE AQUI PRA COLOCAR NO GRÁFICO
  //  RESULTADO VAI SER TIPO ESSE AQUI:
  //   {
  //   k5: {
  //     mongodb: 2257.306,
  //     mysql: 68.246,
  //     neo4j: 322.769,
  //     postgres: 440.799
  //   },
  //   k10: { mongodb: 2053.46, mysql: 70.351, neo4j: 251.5, postgres: 60.126 },
  //   k15: {
  //     mongodb: 2043.918,
  //     mysql: 64.565,
  //     neo4j: 243.742,
  //     postgres: 62.369
  //   },
  //   k20: {
  //     mongodb: 2163.919,
  //     mysql: 63.921,
  //     neo4j: 236.978,
  //     postgres: 69.091
  //   }
  // }

  // let data = arrangeVariant(timestemps);
  // console.log(data);
  // return data;
}

// await getKClosestPairsPlot();

// await getDistancePlot();
// await getRadiusPlot();

// async function get

async function getSpatialJoinPlot() {
  const { timestemps } = await getSpatialJoin();
  let map = arrangeNoVariant(timestemps);
  return map;
}
