/*

  Concurrency Anomalies in PostgreSQL
  FROM PosgresSQL to PL/pgSQL 

  QUERY 1

 author: João Raimundo
*/

-- Register a PL/pgSQL as a new procedural language

CREATE LANGUAGE plpgsql;


-- Create the select function for Query 1

CREATE OR REPLACE FUNCTION get_album_id_Q1() 
	RETURNS SETOF INT AS $$

BEGIN
	RETURN QUERY
			SELECT albums.album_id
			FROM (((albums 
				INNER JOIN bands ON albums.band_id = bands.band_id)
				INNER JOIN bands_genre ON bands.band_id = bands_genre.band_id)
				INNER JOIN genres ON bands_genre.genre_id = genres.genre_id)
        		WHERE genres.genre_name = 'Math rock'
       		 	AND albums.release_date >= '1990/01/01'
        		AND albums.release_date <= '1999/12/31'
       	 		AND LENGTH(albums.abstract) > 200
				GROUP BY albums.album_id
        		ORDER BY albums.sales
        		DESC
        		LIMIT 1;
END;
$$ LANGUAGE plpgsql;

			SELECT albums.album_id
			FROM (((albums 
				INNER JOIN bands ON albums.band_id = bands.band_id)
				INNER JOIN bands_genre ON bands.band_id = bands_genre.band_id)
				INNER JOIN genres ON bands_genre.genre_id = genres.genre_id)
        		WHERE genres.genre_name = 'Math rock'
       		 	AND albums.release_date >= '1990/01/01'
        		AND albums.release_date <= '1999/12/31'
       	 		AND LENGTH(albums.abstract) > 200
				GROUP BY albums.album_id
        		ORDER BY albums.sales
        		DESC;


/*
Create the update function for Query 1, 
where the variable 'album_id_Q1' was declared with the output value of get_album_id_Q1() function
*/

CREATE OR REPLACE FUNCTION update_albums_release_date_Q1(
							release_date_update_Q1 albums.release_date%TYPE)
	RETURNS varchar AS $$
DECLARE
 	album_id_Q1 albums.album_id%TYPE;
BEGIN 
	SELECT get_album_id_Q1() INTO album_id_Q1;
	UPDATE albums SET release_date = release_date_update_Q1 WHERE (albums.album_id = album_id_Q1);
	RETURN 'UPDATED SUCCESSFULLY';
END;
$$ LANGUAGE plpgsql;


-- run queries twice, in distinct shells at the same time - DIRTY READ CONSISTENCY ANOMALY OCCURRED

\set AUTCOMMIT off
BEGIN;
SELECT update_albums_release_date_Q1('1980-01-01');
SELECT pg_sleep(10);
SELECT albums.album_id, albums.release_date
	FROM (((albums 
		INNER JOIN bands ON albums.band_id = bands.band_id)
		INNER JOIN bands_genre ON bands.band_id = bands_genre.band_id)
		INNER JOIN genres ON bands_genre.genre_id = genres.genre_id)
        WHERE genres.genre_name = 'Math rock'
       	AND albums.release_date = '1980-01-01'
       	AND LENGTH(albums.abstract) > 200
		GROUP BY albums.album_id
        ORDER BY albums.sales
        DESC;
COMMIT;

SELECT pg_sleep(5);

SELECT albums.album_id
	FROM (((albums 
		INNER JOIN bands ON albums.band_id = bands.band_id)
		INNER JOIN bands_genre ON bands.band_id = bands_genre.band_id)
		INNER JOIN genres ON bands_genre.genre_id = genres.genre_id)
        WHERE genres.genre_name = 'Math rock'
       	AND albums.release_date >= '1990/01/01'
        AND albums.release_date <= '1999/12/31'
       	AND LENGTH(albums.abstract) > 200
		GROUP BY albums.album_id
        ORDER BY albums.sales
        DESC
		LIMIT 5;
\set AUTCOMMIT on
COMMIT;

/*
DELETE FROM albums WHERE albums.album_id >=0;
\COPY "albums" FROM '/home/ray2g/bands_data/albums.csv' DELIMITER E',' CSV HEADER;
*/

-- T1 OUTPUT:

/*

 update_albums_release_date_q1
-------------------------------
 UPDATED SUCCESSFULLY
(1 row)


 album_id | release_date
----------+--------------
    23907 | 1980-01-01
(1 row)


 album_id
----------
    21300
     2379
     2381
    34011
     2377

(5 rows)


-- T2 OUTPUT:

 update_albums_release_date_q1
-------------------------------
 UPDATED SUCCESSFULLY
(1 row)


 album_id | release_date
----------+--------------
    23907 | 1980-01-01
(1 row)


 album_id
----------
    21300
     2379
     2381
    34011
     2377

(5 rows)

*/

/*
DELETE FROM albums WHERE albums.album_id >=0;
\COPY "albums" FROM '/home/ray2g/bands_data/albums.csv' DELIMITER E',' CSV HEADER;
*/

/* -------------------------------
       LOCKS IMPLEMENTATION
   -------------------------------

Create a new update fuction with lock implementation. 
SHARE ROW EXCLUSIVE was used. This mode protects a table against concurrent data changes, 
and is self-exclusive so that only one session can hold it at a time.
*/

CREATE OR REPLACE FUNCTION update_albums_release_date_Q1_lock(
							release_date_update_Q1 albums.release_date%TYPE)
	RETURNS varchar AS $$
DECLARE
 	album_id_Q1 albums.album_id%TYPE;
BEGIN 
	LOCK TABLE albums IN SHARE ROW EXCLUSIVE MODE;
	SELECT get_album_id_Q1() INTO album_id_Q1;
	UPDATE albums SET release_date = release_date_update_Q1 WHERE(albums.album_id = album_id_Q1);
	RETURN 'UPDATED SUCCESSFULLY';
END;
$$ LANGUAGE plpgsql;


-- run the update function with locks twice, in distinct shells at the same time

\set AUTCOMMIT off
BEGIN;
SELECT update_albums_release_date_Q1_lock('1980-01-01');
SELECT pg_sleep(10);
SELECT albums.album_id, albums.release_date
	FROM (((albums 
		INNER JOIN bands ON albums.band_id = bands.band_id)
		INNER JOIN bands_genre ON bands.band_id = bands_genre.band_id)
		INNER JOIN genres ON bands_genre.genre_id = genres.genre_id)
        WHERE genres.genre_name = 'Math rock'
       	AND albums.release_date = '1980-01-01'
       	AND LENGTH(albums.abstract) > 200
		GROUP BY albums.album_id
        ORDER BY albums.sales
        DESC;
COMMIT;

SELECT pg_sleep(5);

SELECT albums.album_id
	FROM (((albums 
		INNER JOIN bands ON albums.band_id = bands.band_id)
		INNER JOIN bands_genre ON bands.band_id = bands_genre.band_id)
		INNER JOIN genres ON bands_genre.genre_id = genres.genre_id)
        WHERE genres.genre_name = 'Math rock'
       	AND albums.release_date >= '1990/01/01'
        AND albums.release_date <= '1999/12/31'
       	AND LENGTH(albums.abstract) > 200
		GROUP BY albums.album_id
        ORDER BY albums.sales
        DESC
		LIMIT 5;
\set AUTCOMMIT on
COMMIT;

-- OUTPUT T1:

/*
 update_albums_release_date_q1_lock
------------------------------------
 UPDATED SUCCESSFULLY

(1 row)


 album_id | release_date
----------+--------------
    23907 | 1980-01-01

(1 row)

 album_id
----------
    21300
     2379
     2381
    34011
     2377

(5 rows)
*/

-- OUTPUT SHELL 2

/*
 update_albums_release_date_q1_lock
------------------------------------
 UPDATED SUCCESSFULLY

(1 row)

 album_id | release_date
----------+--------------
    23907 | 1980-01-01
    21300 | 1980-01-01

(2 rows)

 album_id
----------
     2379
     2381
    34011
     2377
    18069
(5 rows)
*/

/* ---------------------------------------------
        SERIALIZATION LEVELS - DIRTY READ 
   ----------------------------------------------
*/

\set AUTCOMMIT off
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
	SELECT update_albums_release_date_Q1('1980-01-01');
	SELECT pg_sleep(10);
	SELECT albums.album_id, albums.release_date
		FROM (((albums 
			INNER JOIN bands ON albums.band_id = bands.band_id)
			INNER JOIN bands_genre ON bands.band_id = bands_genre.band_id)
			INNER JOIN genres ON bands_genre.genre_id = genres.genre_id)
			WHERE genres.genre_name = 'Math rock'
			AND albums.release_date = '1980-01-01'
			AND LENGTH(albums.abstract) > 200
			GROUP BY albums.album_id
			ORDER BY albums.sales
			DESC;
COMMIT;

SELECT pg_sleep(10);

SELECT albums.album_id
	FROM (((albums 
		INNER JOIN bands ON albums.band_id = bands.band_id)
		INNER JOIN bands_genre ON bands.band_id = bands_genre.band_id)
		INNER JOIN genres ON bands_genre.genre_id = genres.genre_id)
        WHERE genres.genre_name = 'Math rock'
       	AND albums.release_date >= '1990/01/01'
        AND albums.release_date <= '1999/12/31'
       	AND LENGTH(albums.abstract) > 200
		GROUP BY albums.album_id
        ORDER BY albums.sales
        DESC
		LIMIT 5;
\set AUTCOMMIT on
COMMIT;


-- Output T1:

/*
 update_albums_release_date_q1
-------------------------------
 UPDATED SUCCESSFULLY
(1 row)


 album_id | release_date
----------+--------------
    23907 | 1980-01-01
    21300 | 1980-01-01
     2379 | 1980-01-01

(3 rows)

 album_id
----------
     2381
    34011
     2377
    18069
    18067
(5 rows)
*/

-- Output T2:

/*
ERROR:  could not serialize access due to concurrent update
CONTEXT:  SQL statement "UPDATE albums SET release_date = release_date_update_Q1 WHERE (albums.album_id = album_id_Q1)"
PL/pgSQL function update_albums_release_date_q1(date) line 7 at SQL statement
bands_db=!# SELECT pg_sleep(10);
ERROR:  current transaction is aborted, commands ignored until end of transaction block
*/


/* --------------------------------------------
         INDUCE PHANTOM READ PHENOMENA
   --------------------------------------------
*/


-- CREATE SELECT FUNCTION RETURNS A band_id

CREATE OR REPLACE FUNCTION get_band_id_Q1() 
	RETURNS SETOF INT AS $$

BEGIN
	RETURN QUERY
			SELECT albums.band_id
			FROM (((albums 
				INNER JOIN bands ON albums.band_id = bands.band_id)
				INNER JOIN bands_genre ON bands.band_id = bands_genre.band_id)
				INNER JOIN genres ON bands_genre.genre_id = genres.genre_id)
        		WHERE genres.genre_name = 'Math rock'
       		 	AND albums.release_date >= '1990/01/01'
        		AND albums.release_date <= '1999/12/31'
       	 		AND LENGTH(albums.abstract) > 200
				GROUP BY albums.album_id
        		ORDER BY albums.sales
        		DESC
        		LIMIT 1;
END;
$$ LANGUAGE plpgsql;


-- CREATE INSERT FUNCTION

CREATE OR REPLACE FUNCTION insert_new_album(
										album_id_T albums.album_id%TYPE,
										album_name_T albums.album_name%TYPE,
										sales_T albums.sales%TYPE,
										time_T albums.running_time%TYPE,
										date_T albums.release_date%TYPE,
										abstract_T albums.abstract%TYPE)
	RETURNS varchar AS $$
DECLARE
 	band_id_Q1 albums.band_id%TYPE;
BEGIN 
	SELECT get_band_id_Q1() INTO band_id_Q1;
	INSERT INTO albums (album_id,band_id,album_name,sales,running_time,release_date,abstract)
	VALUES (album_id_T,band_id_Q1,album_name_T,sales_T,time_T,date_T,abstract_T);
	RETURN 'INSERTED SUCCESSFULLY';
END;
$$ LANGUAGE plpgsql;


--- RUN in SHELL 1 T1 Queries and in SHELL 2 T2 INSERT Transaction

--- T1:

\set AUTCOMMIT off
BEGIN;
SELECT album_id, band_id, release_date, sales
	FROM albums as A
	WHERE A.release_date >= '1998/08/10'
    AND A.release_date <= '1998/08/17'
	GROUP BY A.album_id, A.band_id, A.release_date, A.sales
	ORDER BY A.sales
	DESC;
SELECT pg_sleep(20);
SELECT album_id, band_id, release_date, sales
	FROM albums as A
	WHERE A.release_date >= '1998/08/10'
    AND A.release_date <= '1998/08/17'
	GROUP BY A.album_id, A.band_id, A.release_date, A.sales
	ORDER BY A.sales
	DESC;
\set AUTCOMMIT on
COMMIT;


--- T2:

\set AUTCOMMIT off
BEGIN;
SELECT insert_new_album('34716','TEST ALBUM','59','30.0','1998/08/16','TEST ALBUM ABSTRACT');
\set AUTCOMMIT on
COMMIT;


-- T1 OUTPUT

/*
 album_id | band_id | release_date | sales
----------+---------+--------------+-------
    11958 |    3074 | 1998-08-12   |  9951
     8551 |    2149 | 1998-08-12   |  9880
     9799 |    2540 | 1998-08-10   |  9636
     4994 |    1277 | 1998-08-12   |  9368
    21629 |    5510 | 1998-08-17   |  9176
    11957 |    3074 | 1998-08-12   |  8466
    15571 |    4083 | 1998-08-12   |  8205
     4008 |    1088 | 1998-08-17   |  7657

(8 rows)

 album_id | band_id | release_date | sales
----------+---------+--------------+-------
    11958 |    3074 | 1998-08-12   |  9951
     8551 |    2149 | 1998-08-12   |  9880
     9799 |    2540 | 1998-08-10   |  9636
     4994 |    1277 | 1998-08-12   |  9368
    21629 |    5510 | 1998-08-17   |  9176
    11957 |    3074 | 1998-08-12   |  8466
    15571 |    4083 | 1998-08-12   |  8205
     4008 |    1088 | 1998-08-17   |  7657
    34716 |    4705 | 1998-08-16   |    59

(9 rows)
*/

-- T2 OUTPUT

/*
   insert_new_album
-----------------------
 INSERTED SUCCESSFULLY
*/


/* -------------------------------------------
      LOCKS IMPLEMENTATION - PHANTOM READ
   -------------------------------------------
*/

 \set AUTCOMMIT off
BEGIN;
LOCK TABLE albums IN EXCLUSIVE MODE;
SELECT album_id, band_id, release_date, sales
	FROM albums as A
	WHERE A.release_date >= '1998/08/10'
    AND A.release_date <= '1998/08/17'
	GROUP BY A.album_id, A.band_id, A.release_date, A.sales
	ORDER BY A.sales
	DESC;
SELECT pg_sleep(20);
SELECT album_id, band_id, release_date, sales
	FROM albums as A
	WHERE A.release_date >= '1998/08/10'
    AND A.release_date <= '1998/08/17'
	GROUP BY A.album_id, A.band_id, A.release_date, A.sales
	ORDER BY A.sales
	DESC;
\set AUTCOMMIT on
COMMIT;


--- T2: It waits until T1 was COMMITED

\set AUTCOMMIT off
BEGIN;
SELECT insert_new_album('34716','TEST ALBUM','59','30.0','1998/08/16','TEST ALBUM ABSTRACT');
\set AUTCOMMIT on
COMMIT;


-- T1 OUTPUT

/*
 album_id | band_id | release_date | sales
----------+---------+--------------+-------
    11958 |    3074 | 1998-08-12   |  9951
     8551 |    2149 | 1998-08-12   |  9880
     9799 |    2540 | 1998-08-10   |  9636
     4994 |    1277 | 1998-08-12   |  9368
    21629 |    5510 | 1998-08-17   |  9176
    11957 |    3074 | 1998-08-12   |  8466
    15571 |    4083 | 1998-08-12   |  8205
     4008 |    1088 | 1998-08-17   |  7657

(8 rows)

 album_id | band_id | release_date | sales
----------+---------+--------------+-------
    11958 |    3074 | 1998-08-12   |  9951
     8551 |    2149 | 1998-08-12   |  9880
     9799 |    2540 | 1998-08-10   |  9636
     4994 |    1277 | 1998-08-12   |  9368
    21629 |    5510 | 1998-08-17   |  9176
    11957 |    3074 | 1998-08-12   |  8466
    15571 |    4083 | 1998-08-12   |  8205
     4008 |    1088 | 1998-08-17   |  7657

(8 rows)
*/

-- T2 OUTPUT AFTER COMMIT T1

/*
   insert_new_album
-----------------------
 INSERTED SUCCESSFULLY

(1 row)
*/

/* -----------------------------------------------
      SERIALIZATION LEVELS - PHANTOM READ
   -----------------------------------------------
*/

 \set AUTCOMMIT off
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT album_id, band_id, release_date, sales
	FROM albums as A
	WHERE A.release_date >= '1998/08/10'
    AND A.release_date <= '1998/08/17'
	GROUP BY A.album_id, A.band_id, A.release_date, A.sales
	ORDER BY A.sales
	DESC;
SELECT pg_sleep(20);
SELECT album_id, band_id, release_date, sales
	FROM albums as A
	WHERE A.release_date >= '1998/08/10'
    AND A.release_date <= '1998/08/17'
	GROUP BY A.album_id, A.band_id, A.release_date, A.sales
	ORDER BY A.sales
	DESC;
\set AUTCOMMIT on
COMMIT;


--- T2:

 \set AUTCOMMIT off
BEGIN;
SELECT insert_new_album('34716','TEST ALBUM','59','30.0','1998/08/16','TEST ALBUM ABSTRACT');
\set AUTCOMMIT on
COMMIT;


-- T1 OUTPUT:

/*

 album_id | band_id | release_date | sales
----------+---------+--------------+-------
    11958 |    3074 | 1998-08-12   |  9951
     8551 |    2149 | 1998-08-12   |  9880
     9799 |    2540 | 1998-08-10   |  9636
     4994 |    1277 | 1998-08-12   |  9368
    21629 |    5510 | 1998-08-17   |  9176
    11957 |    3074 | 1998-08-12   |  8466
    15571 |    4083 | 1998-08-12   |  8205
     4008 |    1088 | 1998-08-17   |  7657

(8 rows)



 album_id | band_id | release_date | sales
----------+---------+--------------+-------
    11958 |    3074 | 1998-08-12   |  9951
     8551 |    2149 | 1998-08-12   |  9880
     9799 |    2540 | 1998-08-10   |  9636
     4994 |    1277 | 1998-08-12   |  9368
    21629 |    5510 | 1998-08-17   |  9176
    11957 |    3074 | 1998-08-12   |  8466
    15571 |    4083 | 1998-08-12   |  8205
     4008 |    1088 | 1998-08-17   |  7657

(8 rows)
*/

-- T2 OUTPUT: Without Reporting Any ERRORS

/*

   insert_new_album
-----------------------
 INSERTED SUCCESSFULLY

(1 row)
*/


