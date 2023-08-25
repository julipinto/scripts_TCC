export const round = (number, precision = 3) => {
  let factor = Math.pow(10, precision);
  return Math.round(number * factor) / factor;
};
