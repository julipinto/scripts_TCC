-- Inicialize as tabelas temporárias
CREATE TEMPORARY TABLE costs (
  node_id BIGINT PRIMARY KEY,
  cost INT
);

CREATE TEMPORARY TABLE visited (
  node_id BIGINT PRIMARY KEY,
  visited BOOLEAN
);

-- Defina o nó de origem e o nó de destino (target_node_id)
-- Digamos que o nó de origem é ${source_node_id} e o nó de destino é ${target_node_id}
-- Inicialize a tabela de custos com um valor alto (exceto para o nó de origem)
INSERT INTO costs (node_id, cost)
SELECT node_id, CASE WHEN node_id = ${source_node_id} THEN 0 ELSE 999999 END
FROM nodes;

-- Inicialize a tabela de nós visitados como falso para todos os nós
INSERT INTO visited (node_id, visited)
SELECT node_id, FALSE
FROM nodes;

-- Comece o loop do algoritmo
WHILE (SELECT COUNT(*) FROM visited WHERE visited = FALSE) > 0 DO
  -- Encontre o nó com o menor custo não visitado
  SET @current_node_id = (SELECT node_id FROM costs WHERE visited = FALSE ORDER BY cost LIMIT 1);

  -- Marque o nó atual como visitado
  UPDATE visited SET visited = TRUE WHERE node_id = @current_node_id;

  -- Atualize os custos dos vizinhos do nó atual
  UPDATE costs AS c
  JOIN way_nodes AS wn ON c.node_id = wn.node_id
  JOIN way_nodes AS wn2 ON wn.way_id = wn2.way_id AND wn2.node_id != wn.node_id
  JOIN nodes AS n ON wn2.node_id = n.node_id
  SET c.cost = LEAST(c.cost, (SELECT cost FROM costs WHERE node_id = @current_node_id) + 1)
  WHERE c.node_id = n.node_id AND visited = FALSE;
  
  -- Se chegarmos ao nó de destino, podemos sair do loop
  IF @current_node_id = ${target_node_id} THEN
    LEAVE;
  END IF;
END WHILE;

-- Agora, a tabela "costs" contém os custos mínimos para chegar a cada nó a partir do nó de origem
-- E você pode usar essa informação para recuperar o menor caminho até o nó de destino

-- @block

SELECT ST_Distance_Sphere(
    ST_GeomFromText('POINT(-38.9736 -12.2663)', 4143), 
    ST_GeomFromText('POINT(-40.5177 -11.1819)', 4143)
) AS distance;

-- @block

SELECT ST_Distance_Sphere(
    ST_GeomFromText('POINT(-38.5023 -12.9716)', 4326),
    ST_GeomFromText('POINT(-46.6333 -23.5505)', 4326)
) AS distance;

-- @block
SELECT ST_Distance_Sphere(
    ST_GeomFromText('POINT(0 0)', 4326),
    ST_GeomFromText('POINT(-46.6333 -23.5505)', 4326)
) AS distance;

-- @block
-- SELECT tag_value, COUNT(tag_value) FROM node_tags GROUP BY tag_value;
-- DESCRIBE node_tags;
SELECT tag_value, tag_key FROM node_tags WHERE tag_value = 'tower';

-- @block
WITH nt1 AS (
    SELECT node_id, tag_key, tag_value
    FROM node_tags
    WHERE tag_key = 'power' AND tag_value = 'tower'
), nt2 AS (
    SELECT node_id, tag_key, tag_value
    FROM node_tags
    WHERE tag_key = 'power' AND tag_value = 'tower'
)
SELECT nt1.node_id, nt2.node_id, ST_Distance_Sphere((SELECT location FROM nodes WHERE node_id = nt1.node_id), (SELECT location FROM nodes WHERE node_id = nt2.node_id)) AS distance
FROM nt1
CROSS JOIN nt2
WHERE nt1.node_id != nt2.node_id
ORDER BY distance ASC
LIMIT 1;

-- @block
WITH tagged_nodes AS (
    SELECT node_id, location
    FROM nodes
    WHERE node_id IN (
        SELECT DISTINCT nt1.node_id
        FROM node_tags nt1
        WHERE nt1.tag_key = 'power' AND nt1.tag_value = 'tower'
    )
)
SELECT
    nt1.node_id AS node_id_1,
    nt2.node_id AS node_id_2,
    ST_Distance_Sphere(nt1.location, nt2.location) AS distance
FROM
    tagged_nodes nt1
CROSS JOIN
    tagged_nodes nt2
WHERE
    nt1.node_id != nt2.node_id
ORDER BY
    distance ASC
LIMIT 1;

-- SELECT 
-- (SELECT n1.node_id, nt.tag_key, nt.tag_value
-- FROM nodes n1
-- LEFT JOIN node_tags nt ON n1.node_id = nt.node_id
-- WHERE nt.tag_key = 'power' AND nt.tag_value = 'tower') AS nt1
-- CROSS JOIN (
--     SELECT n2.node_id, nt2.tag_key, nt2.tag_value
--     FROM nodes n2
--     LEFT JOIN node_tags nt2 ON n2.node_id = nt2.node_id
--     WHERE nt2.tag_key = 'power' AND nt2.tag_value = 'tower'
-- ) AS nt2
-- WHERE nt1.node_id != nt2.node_id;
-- CROSS JOIN (
--     SELECT node_id, tag_key, tag_value
--     FROM node_tags
--     WHERE tag_key = 'power' AND tag_value = 'tower'
-- ) AS nt2
-- WHERE n1.node_id != nt2.node_id;