CREATE TYPE pair_key AS (a integer, b integer);
CREATE TABLE events (id integer PRIMARY KEY, shard_key pair_key);
INSERT INTO events
SELECT i, CASE i % 4
    WHEN 0 THEN ROW(NULL::integer, i)::pair_key
    WHEN 1 THEN ROW(i, NULL::integer)::pair_key
    WHEN 2 THEN ROW(i, i + 1)::pair_key
    ELSE ROW(NULL::integer, NULL::integer)::pair_key END
FROM generate_series(1, 200) AS i;