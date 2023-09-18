export function removeDuplicates(data) {
  const seenPairs = new Set();

  return data.filter((item) => {
    const pair1 = item.node_id1 + '-' + item.node_id2;
    const pair2 = item.node_id2 + '-' + item.node_id1;

    if (!seenPairs.has(pair1) && !seenPairs.has(pair2)) {
      seenPairs.add(pair1);
      seenPairs.add(pair2);
      return true;
    }

    return false;
  });
}
