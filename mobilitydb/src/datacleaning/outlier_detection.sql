-- CREATE TABLE filteredtable AS (SELECT ROW_NUMBER() OVER() AS id, * FROM aisinput ORDER BY aisinput.mmsi, aisinput.t);


 /* Author: https://github.com/YakshHaranwala/PTRAIL/blob/main/ptrail/preprocessing/filters.py
 * Purpose: Remove duplicate row by the identifier of the row =  (key_name, time)
 *
 * Parameters
 * ----------
 * table_name text : name of database object
 * key_name text : column name of column to filter, the first key 
 * t timestamp : time, the second unique key of each row
 * Returns
 * -------
 * Filtered table 
 */
CREATE OR REPLACE FUNCTION remove_duplicate (table_name text,key_name text,t text)
  RETURNS SETOF filteredtable
  LANGUAGE plpgsql AS
$remove_duplicate$
BEGIN
   RETURN QUERY EXECUTE
   format( 'SELECT DISTINCT ON( database.%2$s, database.%3$s) *
			FROM %1$s AS database;'
   ,table_name,key_name,t);
END
$remove_duplicate$;



/* 
 * Author: Kasidi Mwinyi
 * Purpose: Filtering the table with an external fixed threshold
 * 
 * Parameters.     
 * ----------
 * table_name text : name of database object to filter
 * key_name text : column name to filter
 * condition text : "=", "!=", "<=", ">=", "<", ">"
 * threshold DOUBLE PRECISION : threshold value
 * 
 * Returns
 * -------
 * Filtered table 
 */
CREATE OR REPLACE FUNCTION filter_by_treshold(table_name text,key_name text,condition text,threshold DOUBLE PRECISION)
  RETURNS SETOF filteredtable
  LANGUAGE plpgsql AS
$filter_by_treshold$
BEGIN
   RETURN QUERY EXECUTE
   format( 
     'SELECT *
			FROM %1$s AS table_filtered
			WHERE table_filtered.%2$s %3$s %4$s;'
    ,table_name,key_name,condition,threshold);
END
$filter_by_treshold$;


 /*
 * Author: Kasidi Mwinyi
 * Purpose: Filter data to extract a limited area of data
 *
 * Parameters
 * ----------
 * table_name text : name of the table to filter
 * x_name text : name of position x or longitude
 * y_name text : name of position y or latitude
 * xmin float : threshold for the minimum value of x_name
 * xmax float : threshold for the maximum value of x_name
 * y_name text : name of database object
 * ymin float : threshold for minimum value of y_name
 * ymax float : threshold for maximum value of y_name
 *
 * Returns
 * -------
 * Filtered table 
 */
CREATE OR REPLACE FUNCTION bounding_box_filter (table_name text,x_name text, xmin float, xmax float, y_name text, ymin float, ymax float)
  RETURNS SETOF filteredtable
  LANGUAGE plpgsql AS
$bounding_box_filter$
BEGIN
   RETURN QUERY EXECUTE
   format( 
     'SELECT *
			FROM %1$s AS database
			ORDER BY database.%2$s BETWEEN %3$s AND %4$s AND database.%5$s BETWEEN %6$s AND %7$s;' 
			,table_name,x_name,xmin, xmax, y_name,  ymin, ymax);
END
$bounding_box_filter$;


 /*
 * Author: Kasidi Mwinyi ( inspired by https://github.com/YakshHaranwala/PTRAIL/blob/main/ptrail/preprocessing/filters.py )
 * Purpose: filter data by IQR method
 *
 * Parameters
 * ----------
 * table_name text : name of the table to filter
 * column_name text : column name of table to filter
 *
 * Returns
 * -------
 * Filtered table 
 */

CREATE OR REPLACE FUNCTION iqr_filter(table_name text,column_name text)
  RETURNS SETOF filteredtable
  LANGUAGE plpgsql AS
$iqr_filter$
BEGIN
   RETURN QUERY EXECUTE
   format( 'WITH q AS ( 
				SELECT 
					perce.q_high  + (perce.q_high - perce.q_low)*1.5 AS higher,
					perce.q_low - (perce.q_high - perce.q_low)*1.5 AS lower 
				FROM ( 
					SELECT percentile_disc(0.25) within group (order by database.%2$s) AS q_low, 
							percentile_disc(0.75) within group (order by database.%2$s) AS q_high  
					FROM %1$s AS database
					) AS perce
				)
			SELECT *
			FROM %1$s AS table_name
			WHERE table_name.%2$s <= (SELECT higher FROM q) AND table_name.%2$s >= (SELECT lower FROM q)  ;'
	,table_name,column_name);
			END	
$iqr_filter$;


 



/*
 * Author: Kasidi Mwinyi
 * Purpose: filter data with z score threshold = 3
 * Outlier formula:
 * |    z = (measure - mean)/std_dev 
 * |    z < 3
 *
 * Parameters
 * ----------
 * table_name text : name of the table to filter
 * column_name text : column name of table to filter
 *
 * Returns
 * -------
 * Filtered table 
 */

CREATE OR REPLACE FUNCTION z_score_filter(table_name text,column_name text)
  RETURNS SETOF filteredtable
  LANGUAGE plpgsql AS
$z_score_filter$
BEGIN
   RETURN QUERY EXECUTE
   format( 'WITH mean_sd AS (
				SELECT AVG(%1$s.%2$s) as mean, STDDEV(%1$s.%2$s) AS sd 
				FROM %1$s
			),
			z AS ( 
				SELECT %1$s.id, abs(%1$s.%2$s - mean_sd.mean) / mean_sd.sd AS z_score
				FROM %1$s, mean_sd
			)
			SELECT %1$s.*
			FROM %1$s JOIN z on %1$s.id = z.id
			WHERE  z_score < 3;'
		,table_name,column_name);
END
$z_score_filter$;

 

/*
 * Author: Kasidi Mwinyi 
 * Purpose: filter data with standard deviation method
 *
 * Parameters
 * ----------
 * table_name text : name of the table to filter
 * column_name text : column name of column to filter
 *
 * Returns
 * -------
 * Filtered table 
 */

 CREATE OR REPLACE FUNCTION stddev_filter(table_name text,column_name text)
  RETURNS SETOF filteredtable
  LANGUAGE plpgsql AS
$stddev_filter$
BEGIN
   RETURN QUERY EXECUTE
   format( 
     'WITH mean_sd AS (
				SELECT AVG (database.%2$s) AS mean, STDDEV(database.%2$s) AS sd  
				FROM %1$s AS database
			),stddev_bound AS ( 
				SELECT 
					mean_sd.mean + 3 * mean_sd.sd AS upper_limit,
					mean_sd.mean - 3 * mean_sd.sd AS lower_limit 
				FROM mean_sd
			)
			SELECT database.*
			FROM %1$s AS database, stddev_bound
			WHERE database.%2$s <= stddev_bound.upper_limit AND database.%2$s >= stddev_bound.lower_limit;'
	,table_name,column_name);
			END	
$stddev_filter$;

------------------
 /*
 * Author: Kasidi Mwinyi
 * Purpose: Get median absolute deviation of one table for (2 * windows_size + 1) rows
 *
 * Parameters
 * ----------
 * table_name text : name of table to filter
 * id bigint : id of the row
 * windows_size int : window size for the calcul of median
 * key_value bigint : key of row
 *
 * Returns
 * -------
 * Filtered table 
 */

CREATE OR REPLACE FUNCTION get_partial_median( table_name text, id bigint, windows_size int, key_value bigint)
  RETURNS SETOF DOUBLE PRECISION
  LANGUAGE plpgsql AS
$get_partial_median$
BEGIN
   RETURN QUERY EXECUTE
   format( 'SELECT percentile_disc(0.5) within group( ORDER BY %1$s.propriety ) AS medi
			FROM %1$s
			WHERE %1$s.id BETWEEN %2$s-%3$s AND %2$s+%3$s AND %1$s.key_value=%4$s AND %1$s.propriety is not null;'
			,table_name,id,windows_size,key_value);
END
$get_partial_median$;


 /*
 * Author: Kasidi Mwinyi
 * Purpose: Get median absolute deviation of one table of size 2 * windows_size + 1
 *
 * Parameters
 * ----------
 * table_name text : name of table to filter
 * key_value DOUBLE PRECISION :
 * median DOUBLE PRECISION : median value 
 * id bigint : id of the row
 * windows_size int : window size for the calcul of median
 *
 * Returns
 * -------
 * Filtered table
 */
CREATE OR REPLACE FUNCTION get_partial_mad(table_name text,key_value DOUBLE PRECISION, median DOUBLE PRECISION ,id bigint,windows_size int)
  RETURNS SETOF DOUBLE PRECISION
  LANGUAGE plpgsql AS
$get_partial_mad$
BEGIN
   RETURN QUERY EXECUTE
   format( 'WITH mad_diff AS (
				SELECT ABS(%1$s.propriety - %5$s) AS diff_median
				FROM %1$s
				WHERE %5$s is not null AND %1$s.propriety is not null AND %1$s.id BETWEEN %2$s-%3$s AND %2$s+%3$s AND %1$s.key_value=%4$s 
			)
			SELECT percentile_disc(0.5) within group( ORDER BY mad_diff.diff_median ) AS madi
			FROM mad_diff;
			',table_name,id,windows_size,key_value,median);
END
$get_partial_mad$;


 /*
 * Author: Kasidi Mwinyi
 * Purpose: Detect all outlier with hampel filter method
 *
 * Parameters
 * ----------
 * table_name text : name of 
 * windows_size text : window size for the calcul of median 
 *
 * Returns
 * -------
 * Filtered table 
 */
CREATE OR REPLACE FUNCTION hampel_filter(table_name text,windows_size int)
  RETURNS TABLE( id bigint, median DOUBLE PRECISION  ) 
  LANGUAGE plpgsql AS
$hampel_filter$
BEGIN
   RETURN QUERY EXECUTE
   format( 'WITH median_table AS (
				SELECT *, get_partial_median(''%1$s'',%1$s.id,%2$s,%1$s.key_value) AS median
				FROM %1$s
			), mad_table AS (
				SELECT  *, get_partial_mad(''%1$s'',median_table.key_value,median_table.median,median_table.id,%2$s) AS mad
				FROM median_table 
				WHERE median_table.median is not null
			), detection AS (
				SELECT * 
				FROM mad_table 
				WHERE mad_table.median is not null AND ABS(mad_table.propriety - mad_table.median) > %2$s *  1.4826 * mad_table.mad
			)
			SELECT detection.id , detection.median FROM detection;'
			,table_name,windows_size);
END
$hampel_filter$;




------------------

-- DROP TABLE KalmanState;
-- CREATE TABLE KalmanState( id serial primary key, x float NULL, cov float NULL, predX float NULL, predCov float NULL, K float NULL, identifier bigint NOT NULL );
--drop function kalman_filter(bigint, double precision);

 /*
 * Author: readapted from https://github.com/wouterbulten/kalmanjs/blob/master/contrib/sql/sp_kalman.sql
 *        This code is translated directly from Kalman.js by wouterbulten
 *        https://github.com/wouterbulten/kalmanjs
 *        https://github.com/wouterbulten/kalmanjs/blob/713bb61799fe508a79868a123c916db75ee7a777/dist/kalman.js#L84
 *
 * Purpose: Filter data by kalman filter
 
 * Parameters
 * ----------
 * ident bigint : identifier of an object
 * z DOUBLE PRECISION : position value of one column
 *
 * Returns
 * -------
 * prediction
 */
CREATE OR REPLACE FUNCTION kalman_filter(ident bigint,z DOUBLE PRECISION, OUT prediction DOUBLE PRECISION )
  RETURNS SETOF FLOAT
  LANGUAGE plpgsql AS
$kalman_filter$
DECLARE 

	-- Declare variables to be used in Kalman Filter
	DECLARE R FLOAT = 1;
	DECLARE Q FLOAT = 1;
	DECLARE A FLOAT = 1;
	DECLARE B FLOAT = 0;
	DECLARE C FLOAT = 1;

	DECLARE cov FLOAT;
	DECLARE predX FLOAT;
	DECLARE predCov FLOAT;
	DECLARE K FLOAT;

	DECLARE x FLOAT = NULL;
	DECLARE u FLOAT = 0.0;

BEGIN

	-- Populate existing state

	SELECT k.x, k.cov, k.K, k.predX, k.predCov  INTO x , cov ,K , predX , predCov
	FROM KalmanState k
	WHERE k.identifier = ident
	ORDER BY k.id DESC 
	LIMIT 1;


	IF (x IS NULL) THEN 
		SELECT 1 / C * z ,  1 / C * Q * (1 / C)  INTO x  ,cov;
	ELSE
		SELECT  A * cov * A + R ,A * x + B * u , predCov * C * (1 / (C * predCov * C + Q)) , predX + K * (z - C * predX), predCov - K * C * predCov INTO predX, predCov, K , x , cov;
	END IF;
	-- Clear any existing state
	DELETE FROM KalmanState WHERE KalmanState.identifier = ident;

	-- Save state
	INSERT INTO KalmanState
	VALUES
	(
		x
		,cov
		,predX
		,predCov
		,K
		,ident
	);
  
	
	SELECT x INTO prediction;
	   
END
$kalman_filter$;



-----------------------

