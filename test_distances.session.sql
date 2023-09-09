-- @block
SELECT ST_Distance_Sphere(
  POINT(0, 90), 
  POINT(0, -90)
) AS latitudinal_distance_pole_to_pole;

-- @block
SELECT ST_Distance_Sphere(
  POINT(0, 0), 
  POINT(180, 0)
) AS longitudinal_around_half_the_globe,
longitudinal_around_half_the_globe/pi() AS ratio_of_the_globe;

-- @block
SELECT longitudinal_around_half_the_globe, longitudinal_around_half_the_globe/pi() AS ratio_of_the_globe
FROM (
  SELECT ST_Distance_Sphere(
    POINT(0, 0), 
    POINT(180, 0)
  ) AS longitudinal_around_half_the_globe
) AS subquery;

-- @block
-- SELECT ST_Distance_Sphere(
--   POINT(0, 0), 
--   POINT(180, 0)
-- ) AS longitudinal_around_half_the_globe,
-- longitudinal_around_half_the_globe/pi() AS ratio_of_the_globe;

-- @block
SELECT ST_Distance_Sphere(
    ST_GeomFromText('POINT(0 0)', 0),
    ST_GeomFromText('POINT(180 0)', 0)
) AS longitudinal_around_the_globe;

-- @block
SELECT ST_Distance_Sphere(
    ST_GeomFromText('POINT(0 0)', 4326),
    ST_GeomFromText('POINT(0 180)', 4326)
) AS distance;