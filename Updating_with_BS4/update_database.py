a = """
CREATE TEMP TABLE tmp_x (id int, apple text, banana text); -- but see below

COPY tmp_x FROM '/absolute/path/to/file' (FORMAT csv);

UPDATE tbl
SET    banana = tmp_x.banana
FROM   tmp_x
WHERE  tbl.id = tmp_x.id;

DROP TABLE tmp_x;
"""