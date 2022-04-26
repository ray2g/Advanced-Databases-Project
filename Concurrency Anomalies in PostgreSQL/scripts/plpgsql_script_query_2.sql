/*

  Concurrency Anomalies in PostgreSQL
  FROM PosgresSQL to PL/pgSQL 

  QUERY 2

 author: João Rato
*/

-- register a pl/pgsql as new procedural language 

CREATE LANGUAGE plpgsql;

-- create the select function of query 2

CREATE OR REPLACE FUNCTION get_album_id_most_sales_Q2()
	RETURNS SETOF INT AS $$
BEGIN 
	RETURN QUERY
		SELECT album_id
		FROM albums
		WHERE running_time >'45'
		AND release_date >= '2000/01/01'
		AND release_date >= '2010/12/31'
		ORDER BY sales
		DESC
		LIMIT 1;

END;
$$ LANGUAGE plpgsql;

/*
The query 2 is composed of 2 parts, the select and update queries, where we built 2 functions for each of these parts.
In the select function we used the same query without any alterations where we make a return of an intenger which is the album ID
For the update function we create a value "album_id_before" which has the same type as the column album_id from albums table (int) and we 
proceed to call the select function and saved the result in that variable, after that we update the album with the same ID
equal to the value obtained from the select to 0.
*/

-- update de sales from the obtained result from the select function

CREATE OR REPLACE FUNCTION update_sales_Q2()
	RETURNS varchar AS $$
DECLARE 
	album_id_before albums.album_id%TYPE;
BEGIN 
		SELECT get_album_id_most_sales() INTO album_id_before;
		PERFORM pg_sleep(10);
		UPDATE albums SET sales = 0 WHERE (albums.album_id = album_id_before);
		RETURN 'Update SUCCESSFULL';
END;
$$ LANGUAGE plpgsql;

/*
Now that we have the query 2 functions declared we going to execute the following set of commands in 2 seperate shells, where
after the BEGIN clause we make to selects, the first one the update function and the second a select to check the changes to the
database, after that we put the shell to sleep and check the final results.
For this query we will have to shells reading and updating the same album id, which will cause a dirty read, since the shell 2
is reading an old value and not waiting for the update on shell 1 to end resulting on only one album id getting the sales updated.
*/

/* --------------------------------
          TRANSACTION
   --------------------------------
*/

\set AUTCOMMIT off
BEGIN;
	SELECT update_sales_Q2();
	SELECT band_id, sales, release_date, running_time FROM albums WHERE sales = 0;
COMMIT;

SELECT pg_sleep(3);

SELECT band_id, sales, release_date, running_time FROM albums WHERE sales = 0;
\set AUTCOMMIT on
COMMIT;


--- OUTPUTS: SAME RESULT FOR T1 AND T2


 update_sales_Q2
-------------------------------
    UPDATED SUCCESSFULLY

(1 row)


| band_id | sales | release_date | running_time |
+---------+-------+--------------+--------------+
|     464 |     0 | 2015-06-11   |    76.933334 | 
|     595 |     0 | 2002-12-11   |    42.416668 |
|    2700 |     0 | 1991-06-14   |    51.216667 |

(3 row)


| band_id | sales | release_date | running_time |
+---------+-------+--------------+--------------+
|     464 |     0 | 2015-06-11   |    76.933334 | 
|     595 |     0 | 2002-12-11   |    42.416668 |
|    2700 |     0 | 1991-06-14   |    51.216667 |

(3 row)


/*
To solve this problem we started by adding explicit locks which control 
concurrent access to data in tables. Usually the MVCC takes care of this issues 
however sometimes its required to add explicit locks to obtain the desired result. 
In the following query we added a line where we locked the table albums, 
this means no other transactions can neither read, write, delete or make updates in 
this table while the first transaction does not end.  
*/

-- UPDATE FUNCTION

CREATE OR REPLACE FUNCTION update_sales_lock_Q2()
	RETURNS varchar AS $$
DECLARE 
	album_id_before albums.album_id%TYPE;
BEGIN 
		LOCK TABLE albums;
		SELECT get_album_id_most_sales() INTO album_id_before;
		PERFORM pg_sleep(10);
		UPDATE albums SET sales = 0 WHERE (albums.album_id = album_id_before);
		RETURN 'Update SUCCESSFULL';
END;
$$ LANGUAGE plpgsql;


-- TRANSACTION - LOCKS

\set AUTOCOMMIT off
BEGIN;
	SELECT update_sales_lock_Q2();
	SELECT band_id,album_id, sales, release_date, running_time FROM albums WHERE sales = 0;
COMMIT;

SELECT pg_sleep(3);

SELECT band_id,album_id, sales, release_date, running_time FROM albums WHERE sales = 0;
\set AUTOCOMMIT on
COMMIT;


--- OUTPUTS:

--- T1:

 update_sales_lock_Q2
-------------------------------
    UPDATED SUCCESSFULLY

(1 row)


band_id | album_id | sales | release_date | running_time 
---------+----------+-------+--------------+--------------
     595 |     2564 |     0 | 2002-12-11   |    42.416668
    2700 |    10885 |     0 | 1991-06-14   |    51.216667
     464 |     1280 |     0 | 2015-06-11   |    76.933334

(3 row)


band_id | album_id | sales | release_date | running_time 
---------+----------+-------+--------------+--------------
     595 |     2564 |     0 | 2002-12-11   |    42.416668
    2700 |    10885 |     0 | 1991-06-14   |    51.216667
     464 |     1280 |     0 | 2015-06-11   |    76.933334

(3 row)



--- T2:


 update_sales_lock_Q2
-------------------------------
    UPDATED SUCCESSFULLY

(1 row)


band_id | album_id | sales | release_date | running_time 
---------+----------+-------+--------------+--------------
     595 |     2564 |     0 | 2002-12-11   |    42.416668
    2700 |    10885 |     0 | 1991-06-14   |    51.216667
     464 |     1280 |     0 | 2015-06-11   |    76.933334
     464 |     1338 |     0 | 2015-06-11   |     79.13333

(4 row)


band_id | album_id | sales | release_date | running_time 
---------+----------+-------+--------------+--------------
     595 |     2564 |     0 | 2002-12-11   |    42.416668
    2700 |    10885 |     0 | 1991-06-14   |    51.216667
     464 |     1280 |     0 | 2015-06-11   |    76.933334
     464 |     1338 |     0 | 2015-06-11   |     79.13333

(4 row)



While the first transaction is not finished the second one is on standby and when the first completes, the second starts and can prooceed the transaction where it will get the most recent values. In figure x and y its possible to check that the dirty read is solver and 2 rows were updated ( band_id 464 and album_id 1280 & 1338).

/*
ISOLATION LEVEL Read committed
Taking in consideration the phenomena ocurred with the query 2 and the figure ( figura dos diferentes niveis de isolamento) 
we can see that the read uncommited is the best isolation level to use in our query since the other levels are more restrict, 
however in postgresql read uncommited has the same behaviour as read committed, so internally only read commited is implemented.
In the following figures is possible to see how the isolation level was implemented in our code and aswell the results.
*/


--- TRANSACTION - ISOLATION LEVEL

\set AUTOCOMMIT off
BEGIN TRANSACTION ISOLATION LEVEL Read Committed;
	SELECT update_sales_Q2();
	SELECT band_id, sales, release_date, running_time FROM albums WHERE sales = 0;
COMMIT;

SELECT pg_sleep(10);

SELECT band_id, sales, release_date, running_time FROM albums WHERE sales = 0;
\set AUTOCOMMIT on
COMMIT;
	

--- OUTPUTS

--- T1: 

 update_sales_Q2
-------------------------------
    UPDATED SUCCESSFULLY

(1 row)


band_id | album_id | sales | release_date | running_time 
--------+----------+-------+--------------+--------------
    464 |     1280 |     0 | 2015-06-11   |    76.933334

(1 row)


band_id | album_id | sales | release_date | running_time 
--------+----------+-------+--------------+--------------
    464 |     1280 |     0 | 2015-06-11   |    76.933334

(1 row)


--- T2:

 update_sales_Q2
-------------------------------
    UPDATED SUCCESSFULLY

(1 row)


band_id | album_id | sales | release_date | running_time 
--------+----------+-------+--------------+--------------
    464 |     1280 |     0 | 2015-06-11   |    76.933334

(1 row)


band_id | album_id | sales | release_date | running_time 
--------+----------+-------+--------------+--------------
    464 |     1280 |     0 | 2015-06-11   |    76.933334
    464 |     1338 |     0 | 2015-06-11   |     79.13333

(2 row)

/*
As you can see, the first and second shell ended with 1 changed rows meaning it was sucessfull, this happens because
read commited checks if theres other transactions ocurring at the same time for the same piece of data that is being updated
and after the first one is updated the second transaction checks if the result of the first transaction is still aplies 
to the data we are selecting, if it does apply then it would update the same piece of data, but in our case
the data gets updated to 0 and no longer satisfies the condtion of the highest sales, so the second transaction checks again
which part of the data satisfies the condition and performs the update.
*/

/* -------------------------------------------------
     INDUCING LOST UPDATE PHENOMENA
   -------------------------------------------------
*/


-- UPDATE FUNCTION

CREATE OR REPLACE FUNCTION update_sales_lost_update()
	RETURNS varchar AS $$
DECLARE 
	album_id_before albums.album_id%TYPE;
BEGIN 
		SELECT get_album_id_most_sales() INTO album_id_before;
		PERFORM pg_sleep(10);
		UPDATE albums SET sales = 1 WHERE (albums.album_id = album_id_before);
		RETURN 'UPDATE SUCCESSFUL';
END;
$$ LANGUAGE plpgsql;


-- TRANSACTION 

\set AUTCOMMIT off
BEGIN;
	SELECT update_sales_lost_update_Q2();
	SELECT band_id, sales, release_date, running_time FROM albums WHERE sales = 1;
COMMIT;

SELECT pg_sleep(10);

SELECT band_id, sales, release_date, running_time FROM albums WHERE sales = 1;
\set AUTCOMMIT on


--- OUTPUTS -  T2 OVERWRITES T1

--- T1: 

 update_sales_lost_update_Q2
-------------------------------
    UPDATED SUCCESSFULLY

(1 row)


| band_id | sales | release_date | running_time |
+---------+-------+--------------+--------------+
|     464 |     0 | 2015-06-11   |    76.933334 | 

(1 row)


| band_id | sales | release_date | running_time |
+---------+-------+--------------+--------------+
|     464 |     1 | 2015-06-11   |    76.933334 | 

(1 row)


--- T2: 

 update_sales_lost_update_Q2
-------------------------------
    UPDATED SUCCESSFULLY

(1 row)


| band_id | sales | release_date | running_time |
+---------+-------+--------------+--------------+
|     464 |     1 | 2015-06-11   |    76.933334 | 

(1 row)


| band_id | sales | release_date | running_time |
+---------+-------+--------------+--------------+
|     464 |     1 | 2015-06-11   |    76.933334 | 

(1 row)

/* -----------------------------------------------
        LOCKS - LOST UPDATE
   -----------------------------------------------
*/

CREATE OR REPLACE FUNCTION update_sales_lock_Q2()
	RETURNS varchar AS $$
DECLARE 
	album_id_before albums.album_id%TYPE;
BEGIN 
		LOCK TABLE albums;
		SELECT get_album_id_most_sales() INTO album_id_before;
		PERFORM pg_sleep(10);
		UPDATE albums SET sales = 0 WHERE (albums.album_id = album_id_before);
		RETURN 'UPDATE SUCCESSFUL';
END;
$$ LANGUAGE plpgsql;


/* ----------------------------------
      TRANSACTIONS - LOST UPDATE
   ----------------------------------
*/


--- T1:

\set AUTOCOMMIT off
BEGIN;
	SELECT update_sales_lock_Q2();
	SELECT band_id,album_id, sales, release_date, running_time FROM albums WHERE sales = 0;
COMMIT;

SELECT pg_sleep(10);

SELECT band_id,album_id, sales, release_date, running_time FROM albums WHERE sales = 0;
\set AUTOCOMMIT on


--- T2:

\set AUTOCOMMIT off
BEGIN;
	SELECT update_sales_lost_update_Q2();
	SELECT band_id, sales, release_date, running_time FROM albums WHERE sales= 0 or sales = 1;
COMMIT;

SELECT pg_sleep(10);

SELECT band_id, sales, release_date, running_time FROM albums WHERE sales = 0 or sales = 1;
\set AUTOCOMMIT on 


/* --------------------------------------------
         OUTPUTS - LOST UPDATE - LOCKS
   --------------------------------------------
*/

--- T1:

update_sales_lock_Q2
-------------------------------
    UPDATED SUCCESSFULLY

(1 row)


band_id | album_id | sales | release_date | running_time 
--------+----------+-------+--------------+--------------
    464 |     1280 |     0 | 2015-06-11   |    76.933334

(1 row)


band_id | album_id | sales | release_date | running_time 
--------+----------+-------+--------------+--------------
    464 |     1280 |     0 | 2015-06-11   |    76.933334

(1 row)


--- T2:

update_sales_lost_update_Q2
-------------------------------
    UPDATED SUCCESSFULLY

(1 row)


band_id | album_id | sales | release_date | running_time 
--------+----------+-------+--------------+--------------
    464 |     1280 |     0 | 2015-06-11   |    76.933334

(1 row)


band_id | album_id | sales | release_date | running_time 
--------+----------+-------+--------------+--------------
    464 |     1280 |     0 | 2015-06-11   |    76.933334
    464 |     1338 |     1 | 2015-06-11   |     79.13333

(2 row)


/* --------------------------------------------------
      ISOLATION LEVELS - LOST UPDATE
   --------------------------------------------------
*/

--- TRANSACTION

---T1:

\set AUTOCOMMIT off
BEGIN TRANSACTION ISOLATION LEVEL Repeatable read;
	SELECT update_sales_Q2();
	SELECT band_id, sales, release_date, running_time FROM albums WHERE sales = 0;
COMMIT;

SELECT pg_sleep(10);

SELECT band_id, sales, release_date, running_time FROM albums WHERE sales = 0;
\set AUTOCOMMIT on
COMMIT;


---T2:

\set AUTOCOMMIT off
BEGIN;
	SELECT update_sales_lost_update_Q2();
	SELECT band_id, sales, release_date, running_time FROM albums WHERE sales = 1;
COMMIT;

SELECT pg_sleep(10);

SELECT band_id, sales, release_date, running_time FROM albums WHERE sales = 1;
\set AUTOCOMMIT on
COMMIT;


/* ----------------------------------------
          OUTPUTS - ISOLATION LEVEL 
   ----------------------------------------
*/

--- T1:

 update_sales_Q2
-------------------------------
    UPDATED SUCCESSFULLY

(1 row)


band_id | album_id | sales | release_date | running_time 
--------+----------+-------+--------------+--------------
    464 |     1280 |     0 | 2015-06-11   |    76.933334

(1 row)


band_id | album_id | sales | release_date | running_time 
--------+----------+-------+--------------+--------------
    464 |     1280 |     0 | 2015-06-11   |    76.933334

(1 row)

--- T2:

bands=*# SELECT update_sales_isolation();
ERROR:  could not serialize access due to concurrent update
CONTEXT:  SQL statement "UPDATE albums SET sales = 1 WHERE (albums.album_id = album_id_before)"
PL/pgSQL function update_sales_isolation() line 7 at SQL statement

