##Example filter_outlier_by_treshold

```postgresql
drop table test;
CREATE TABLE test AS (SELECT * FROM filter_outlier_by_treshold('aisinput','SOG','<=',1));
```

## Example filter_by_id

```postgresql
drop table test;
CREATE TABLE test AS (SELECT * FROM filter_by_id('aisinput','mmsi',2190048));
```

## Example filter_bounding_box

```postgresql
CREATE TABLE aisinputbb AS SELECT filter_bounding_box('aisinput','longitude',-16.1,32.88,'latitude',40.18,84.17);
```

## Example filter_outlier_with_iq

```postgresql
drop table test;
CREATE TABLE test AS (SELECT * FROM filter_outlier_with_iq('aisinput','SOG'));
```

## Example remove_duplicate

```postgresql
CREATE TABLE test AS (SELECT * FROM remove_duplicate('aisinput','mmsi','t'));
```

## Example hampel_filter_outlier_detection

```postgresql
-- database preparation
DROP TABLE hamplefilteredtable;

CREATE TABLE hamplefilteredtable AS ( SELECT ROW_NUMBER() OVER() AS id, aisinput.mmsi AS key_value, aisinput.sog AS propriety FROM aisinput ORDER BY(aisinput.mmsi, aisinput.t)  );

-- data correction 
UPDATE hamplefilteredtable
SET propriety = h.median
FROM  hampel_filter_outlier_detection('hamplefilteredtable',3) h
WHERE hamplefilteredtable.id = h.id;
```


## Example kalman_filter_outlier_detection

```postgresql
SELECT kalman_filter(kalman_table.id, kalman_table.latitude) FROM kalman_table;
```
 
 ## Example z_score_filter_outlier_detection
 
```postgresql
SELECT * FROM z_score_filter('kalmanfilteredtable','mmsi','sog');
```
 
 ## Example filter_outlier_with_stddev

```postgresql
SELECT * FROM filter_outlier_with_stddev('stddev_table','sog');
```
