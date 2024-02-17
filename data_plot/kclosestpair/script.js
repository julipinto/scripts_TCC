import k5 from './k5.js';
import k10 from './k10.js';
import k15 from './k15.js';
import k20 from './k20.js';

let ks = [k5, k10, k15, k20];

let map = {};

for (let k of ks) {
  for (let [dataset, databases] of Object.entries(k)) {
    for (let [database, [time]] of Object.entries(databases)) {
      if (!(dataset in map)) map[dataset] = {};
      if (!(database in map[dataset])) map[dataset][database] = [];
      map[dataset][database].push(time);
    }
  }
}

console.log(JSON.stringify(map));
