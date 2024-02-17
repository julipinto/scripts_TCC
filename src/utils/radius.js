import drawCircle from 'circle-to-polygon';


let circle = drawCircle([
  -38.97193735258372,
  -12.198091864715906
], 6000, 32);

let c =    {
  type: 'Feature',
  geometry: {
    type: 'Polygon',
    coordinates: circle.coordinates,
  },
  properties: { id: 'radius' },
}

console.log(JSON.stringify(c))