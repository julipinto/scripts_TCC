SELECT ST_DistanceSphere(
    ST_GeomFromText('POINT(-38.9736 -12.2663)', 4326),
    ST_GeomFromText('POINT(-40.5177 -11.1819)', 4326)
) AS distance;

-- @block
SELECT Count(srid) FROM spatial_ref_sys;


-- @block
SELECT ST_DistanceSphere(
    ST_GeomFromText('POINT(-38.5023 -12.9716)', 0),
    ST_GeomFromText('POINT(-46.6333 -23.5505)', 0)
) AS distance;

-- @block
WITH tagged_nodes AS (
    SELECT n.node_id, n.location
    FROM nodes n
    WHERE n.node_id IN (
        SELECT DISTINCT nt1.node_id
        FROM node_tags nt1
        WHERE nt1.tag_key = 'power' AND nt1.tag_value = 'tower'
    )
)
SELECT
    nt1.node_id AS node_id_1,
    nt2.node_id AS node_id_2,
    ST_DistanceSphere(nt1.location::geometry, nt2.location::geometry) AS distance
FROM
    tagged_nodes nt1
CROSS JOIN
    tagged_nodes nt2
WHERE
    nt1.node_id != nt2.node_id
ORDER BY
    distance ASC
LIMIT 1;

-- @block
-- SELECT
--     ST_DistanceSphere(
--         ST_MakePoint(0, 0),
--         ST_MakePoint(180, 0)
--     ) AS longitudinal_around_half_the_globe,
--     ST_DistanceSphere(
--         ST_MakePoint(0, 0),
--         ST_MakePoint(180, 0)
--     ) / pi() AS ratio_of_the_globe;

SELECT distance, distance/pi() AS ratio_of_the_globe
FROM 
    (SELECT
        ST_DistanceSphere(
            ST_GeomFromText('POINT(0 0)', 0),
            ST_GeomFromText('POINT(180 0)', 0)
        ) AS distance
    ) AS subquery;

-- @block

SELECT distance, distance/pi() AS ratio_of_the_globe
FROM 
    (SELECT
        ST_DistanceSphere(
            ST_GeomFromText('POINT(0 0)', 0),
            ST_GeomFromText('POINT(180 0)', 0)
        ) AS distance
    ) AS subquery;




