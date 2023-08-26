SELECT n.node_id, n.location
    FROM nodes n
    JOIN nodes central_node ON central_node.node_id = 4662482749
    WHERE ST_DWithin(n.location, central_node.location, 30);

-- SELECT n.node_id, n.location
-- FROM nodes n
-- JOIN nodes central_node ON central_node.node_id = ${node1}
-- WHERE ST_DWithin(n.location, central_node.location, ${radius});