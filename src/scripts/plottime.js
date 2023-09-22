import {
  getDistanceResults,
  getRadiusResults,
  getWindowResults,
  getRangeCountResults,
  getKClosestPairs,
  getKNNResults,
} from './results.js';

function arrangeNoVariant(timestemps) {
  let data = {};
  for (let [_, all] of Object.entries(timestemps)) {
    for (let [database, time] of Object.entries(all)) {
      if (!(database in data)) data[database] = [];
      data[database].push(time);
    }
  }
  return data;
}

// 1 variante
async function getDistancePlot() {
  const { timestemps } = await getDistanceResults();
  let data = arrangeNoVariant(timestemps);
}

// 4 variantes
async function getRadiusPlot() {
  const { timestemps } = await getRadiusResults();
  // let data = arrangeNoVariant(timestemps);
  // console.log({ data });
}

// 1 variante
async function getWindowPlot() {
  const { timestemps } = await getWindowResults();
  let data = arrangeNoVariant(timestemps);
}

// 1 variante + 4 variante
async function getRangeCountPlot() {
  const { timestempsRadius, timestempsWindow } = await getRangeCountResults();
  // let data = arrangeNoVariant(timestemps);
}

// 4 variantes
async function getKNNPlot() {
  const { timestemps } = await getKNNResults();
  // let data = arrangeNoVariant(timestemps);
}

// 1 variante
async function getKClosestPairsPlot() {
  const { timestemps } = await getKClosestPairs();
  let data = arrangeNoVariant(timestemps);
}

// await getDistancePlot();
// await getRadiusPlot();
