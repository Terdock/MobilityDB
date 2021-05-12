/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 *
 * Copyright (c) 2016-2021, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS. 
 *
 *****************************************************************************/

DROP FUNCTION IF EXISTS create_test_tables_big_dup();
CREATE OR REPLACE FUNCTION create_test_tables_big_dup(size int DEFAULT 10000)
RETURNS text AS $$
DECLARE
  perc int;
BEGIN
perc := size * 0.5;
IF perc < 1 THEN perc := 1; END IF;

-----------------------------------------------------------

DROP TABLE IF EXISTS tbl_timestampset_test;
CREATE TABLE tbl_timestampset_test AS
SELECT k, random_timestampset('2001-01-01', '2001-12-31', 10, 5, 10) AS ti
FROM generate_series(1, size) k;
/* Add perc duplicates */
INSERT INTO tbl_timestampset_test
SELECT i + 50, timestampset '{2001-01-01, 2001-02-01, 2001-03-01}'
FROM generate_series(1, perc) i;

DROP INDEX IF EXISTS tbl_timestampset_test_gist_idx;
CREATE INDEX tbl_timestampset_test_gist_idx ON tbl_timestampset_test USING gist(ti);

-- DROP INDEX IF EXISTS tbl_timestampset_test_spgist_idx;
-- CREATE INDEX tbl_timestampset_test_spgist_idx ON tbl_timestampset_test USING spgist(ti);

-- SELECT count(*) FROM tbl_timestampset_test
-- WHERE ti && timestampset '{2001-01-01, 2001-02-01, 2001-03-01}';

-----------------------------------------------------------

DROP TABLE IF EXISTS tbl_ttextinstset_test;
CREATE TABLE tbl_ttextinstset_test AS
SELECT k, random_ttextinstset('2001-01-01', '2001-12-31', 10, 10, 5, 10) AS ti
FROM generate_series(1, size) k;
/* Add perc duplicates */
INSERT INTO tbl_ttextinstset_test
SELECT i + 50, ttext '{AA@2001-01-01, DD@2001-02-01, FF@2001-03-01}'
FROM generate_series(1, perc) i;

DROP INDEX IF EXISTS tbl_ttextinstset_test_gist_idx;
CREATE INDEX tbl_ttextinstset_test_gist_idx ON tbl_ttextinstset_test USING gist(ti);

-- DROP INDEX IF EXISTS tbl_ttextinstset_test_spgist_idx;
-- CREATE INDEX tbl_ttextinstset_test_spgist_idx ON tbl_ttextinstset_test USING spgist(ti);

-- SELECT count(*) FROM tbl_ttextinstset_test
-- WHERE ti && ttext '{AA@2001-01-01, DD@2001-02-01, FF@2001-03-01}';

-----------------------------------------------------------

DROP TABLE IF EXISTS tbl_tfloatinstset_test;
CREATE TABLE tbl_tfloatinstset_test AS
SELECT k, random_tfloatinstset(0, 100, '2001-01-01', '2001-12-31', 10, 10, 5, 10) AS ti
FROM generate_series(1, size) k;
/* Add perc duplicates */
INSERT INTO tbl_tfloatinstset_test
SELECT i + 50, tfloat '{1@2001-01-01, 10@2001-02-01, 20@2001-03-01}'
FROM generate_series(1, perc) i;

DROP INDEX IF EXISTS tbl_tfloatinstset_test_gist_idx;
CREATE INDEX tbl_tfloatinstset_test_gist_idx ON tbl_tfloatinstset_test USING gist(ti);

-- DROP INDEX IF EXISTS tbl_tfloatinstset_test_spgist_idx;
-- CREATE INDEX tbl_tfloatinstset_test_spgist_idx ON tbl_tfloatinstset_test USING spgist(ti);

-- SELECT count(*) FROM tbl_tfloatinstset_test
-- WHERE ti && tfloat '{1@2001-01-01, 10@2001-02-01, 20@2001-03-01}';

-----------------------------------------------------------

DROP TABLE IF EXISTS tbl_tgeompointinstset_test;
CREATE TABLE tbl_tgeompointinstset_test AS
SELECT k, random_tgeompointinstset(0, 100, 0, 100, '2001-01-01', '2001-12-31', 10, 10, 5, 10) AS ti
FROM generate_series(1, size) k;
/* Add perc duplicates */
INSERT INTO tbl_tgeompointinstset_test
SELECT i + 50, tgeompoint '{Point(1 1)@2001-01-01, Point(10 10)@2001-02-01, Point(20 20)@2001-03-01}'
FROM generate_series(1, perc) i;

DROP INDEX IF EXISTS tbl_tgeompointinstset_test_gist_idx;
CREATE INDEX tbl_tgeompointinstset_test_gist_idx ON tbl_tgeompointinstset_test USING gist(ti);

-- DROP INDEX IF EXISTS tbl_tgeompointinstset_test_spgist_idx;
-- CREATE INDEX tbl_tgeompointinstset_test_spgist_idx ON tbl_tgeompointinstset_test USING spgist(ti);

-- SELECT count(*) FROM tbl_tgeompointinstset_test
-- WHERE ti && tgeompoint '{Point(1 1)@2001-01-01, Point(10 10)@2001-02-01, Point(20 20)@2001-03-01}';

-------------------------------------------------------------------------------
RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';

-- SELECT create_test_tables_big_dup(10000);

