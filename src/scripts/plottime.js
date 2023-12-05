import FileHandler, { dirQueries } from '../utils/FileHandler.js';
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

const sanitizeOutputKey = {
  out_fsa: 'FSA',
  out_ba: 'BA',
  out_ne: 'NE',
  out_br: 'BR',
};

let outs = {
  out_fsa: 'C:\\Users\\nana-\\Documents\\github\\Juliapp\\results_tcc\\out_fsa',
  out_ba: 'C:\\Users\\nana-\\Documents\\github\\Juliapp\\results_tcc\\out_ba',
  out_ne: 'C:\\Users\\nana-\\Documents\\github\\Juliapp\\results_tcc\\out_ne',
  out_br: 'C:\\Users\\nana-\\Documents\\github\\Juliapp\\results_tcc\\out_br',
};

const out_plot = "../data_plot"
const out_plot_file = new FileHandler(out_plot)

// function arrangeNoVariant(timestemps) {
//   let data = {};
//   for (let [_, all] of Object.entries(timestemps)) {
//     for (let [database, time] of Object.entries(all)) {
//       let key = sanitizeDatabaseKey[database];
//       if (!(key in data)) data[key] = [];
//       data[key].push(time);
//     }
//   }
//   return data;
// }

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
  for (let [dataset, result_out] of Object.entries(timestemps)) {
    for (let [_, node_results] of Object.entries(result_out)) {
      for (let [variant, databases_result] of Object.entries(node_results)) {
        for (let [db_key, time] of Object.entries(databases_result)) {
          let database = sanitizeDatabaseKey[db_key];

          if (!(variant in map)) map[variant] = {};
          if (!(dataset in map[variant])) map[variant][dataset] = {};
          if (!(database in map[variant][dataset]))
            map[variant][dataset][database] = [];

          map[variant][dataset][database].push(time);
        }
      }
    }
  }

  return map;
}

function saveVariant(data, queryName) {
  for (let [variant, plots] of Object.entries(data)) {
    out_plot_file.writeOut({
      queryName,
      data: plots,
      filename: variant
    })
  }
}

function saveNoVariant(data, queryName) {
  out_plot_file.writeOut({
    queryName: queryName,
    data: data,
    filename: queryName + "_plot"
  })
}

// 1 variante
async function getDistancePlot() {
  let plots = {};
  for (let [out_key, out] of Object.entries(outs)) {
    const { timestemps } = await getDistanceResults(out);
    let data = arrangeNoVariant(timestemps);
    plots[sanitizeOutputKey[out_key]] = data;
  }

  saveNoVariant(plots, dirQueries.distance)

  return plots;
}

// getDistancePlot();


// 4 variantes ok
async function getRadiusPlot() {
  const timestemps = {};
  for (let [out_key, out] of Object.entries(outs)) {
    timestemps[sanitizeOutputKey[out_key]] = (
      await getRadiusResults(out)
    ).timestemps;
  }

  let map = arrangeVariant(timestemps);

  saveVariant(map, dirQueries.radius);

  return map;
}

// getRadiusPlot();

// 1 variante
async function getWindowPlot() {
  let plots = {};
  for (let [out_key, out] of Object.entries(outs)) {
    const { timestemps } = await getWindowResults(out);
    let data = arrangeNoVariant(timestemps);
    plots[sanitizeOutputKey[out_key]] = data;
  }
  // console.log(JSON.stringify(plots));
  // console.log(plots);
  return plots;
}

// getWindowPlot();

// 1 variante + 4 variante ok
async function getRangeCountPlot() {
  let timestamps_radius = {};
  let plot_window = {};

  for (let [out_key, out] of Object.entries(outs)) {
    const { timestempsRadius, timestempsWindow } = await getRangeCountResults(
      out
    );
    let data_window = arrangeNoVariant(timestempsWindow);
    plot_window[sanitizeOutputKey[out_key]] = data_window;

    timestamps_radius[sanitizeOutputKey[out_key]] = timestempsRadius;
  }

  let plot_radius = arrangeVariant(timestamps_radius);

  saveNoVariant(plot_window, dirQueries.windowCount)
  saveVariant(plot_radius, dirQueries.radiusCount)
}

// await getRangeCountPlot();

// 4 variantes ok
async function getKNNPlot() {
  const timestemps = {};
  for (let [out_key, out] of Object.entries(outs)) {
    timestemps[sanitizeOutputKey[out_key]] = (
      await getKNNResults(out)
    ).timestemps;
  }

  let map = arrangeVariant(timestemps);
  // console.log(map);
  // print_variant(map);
  saveVariant(map, dirQueries.knn)
  return map;
  // let data = arrangeNoVariant(timestemps);
}

// getKNNPlot()

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

//    TEM QUE VER ESSE AQ 🤔
async function getSpatialJoinPlot() {
  let map = {};
  for (let [out_key, out] of Object.entries(outs)) {
    let timestamps = (await getSpatialJoin(out)).timestemps;
    let data = {};
    for (let [district, results] of Object.entries(timestamps)) {
      for (let [database, time] of Object.entries(results)) {
        let key = sanitizeDatabaseKey[database];
        if (!(key in data)) data[key] = [];
        data[key].push(time);
      }
    }
    map[sanitizeOutputKey[out_key]] = data;
  }

  // console.log(JSON.stringify(map))

  saveNoVariant(map, dirQueries.spatialJoin)

  return map;
}

getSpatialJoinPlot();
