import { readdir } from 'fs/promises';

async function readDirs() {
  return await readdir('./out');
}

let dirs = await readDirs();
console.log(dirs);
