<img src="https://ciencias.ulisboa.pt/sites/default/files/Ciencias_Logo_Azul-01.png" width="250" height="130">

# Advanced-Databases-Project
Repository to compile all the project milestones that have been developed in the Advanced Databases curricular unit at FCUL.

<br>

Team:
  * Daniela Vieira
  * João Raimundo
  * João Rato
  * Madalena Vieira
  
Oriented by:
 * Professor Cátia Pesquita

<br> 

# Project Definition
This project part aims at comparing a relational database and a NoSQL database in terms of data modelling, querying, transactions and optimizations.

### Infrastructure:

  * **Relational Database:** PostgreSQL

  * **NoSQL:** MongoDB

### Data:

The data for the project consists of 5 csv files extracted from dbpedia.
  * www.dbpedia.org
  * http://dbpedia.org/page/Radiohead

### Files:

  * band-band_name.csv: Contains 10k music band dbpedia URIs, and band names

  * band-album_data.csv: Contains band URIs and associated album names, along with their abstract, release date, running time (in minutes) and sales amount (if available).

  * band-genre_name.csv: Contains band URIs and genre names associated with the music band

  * band-member-member_name.csv: Contains band URIs, current member URIs and their names associated with the music band

  * band-former_member-member_name.csv: Contains band URIs, former member URIs and their names associated with the music band

<br>

# Part 1: Data Modelling and Querying:

### Tasks:

  1. Write the specifications for two fairly complex data operations that are able to showcase the differences between relational and NoSQL databases
  
    - Example: Insert a new album called "Best Of" for a band that released their first album in the 70s who sold the most albums in the 90s.
    
    - This is a complex operation because it includes multiple queries, includes write and read operations, and includes heavy queries (sort by, group by, range queries).
    
    - The two operations must create possible conflicts when run at the same time, e.g. they read/write the same piece of data
    
  2. Define the relational schema:
  
    - You can draw an Entity-Relationship model
    - You can draw a a Relational Diagram
    - You MUST write the CREATE TABLES statements
    
  3. Build a relational database in Postgres to store your data and implement the operations designed in 1.
  
  4. Build a NoSQL database in the system selected in 1 to store your data and implement the operations designed in 1.

<br>

# Part 2: Reliability and Scalability

This part of the project focuses on the Relational Database and reliability of transactions. You will learn how to code complex operations in a procedural language and then use these in concurrency anomaly experiments. You will learn how to identify, demonstrate and solve concurrency anomalies.

  1. Implement your complex operations in PL/pgSQL:
  
    - Using the operations designed in part 1, implement each of them as part of a single procedure with at least two separate queries.
    - Implement simple WRITE queries that change the data read by the complex operations to support your concurrency experiments.
   
  2. Concurrency anomalies experiments:
  
    - Identify the types of concurrency anomalies that concurrent execution of your operations could result in
    - Demonstrate concurrency issues by changing isolation levels in Postgres and using the sleep function inside the procedure
      
  3. Solve the concurrency anomalies using locks inside your procedures
  
  5. Compare the use locks and isolation level settings.

<br>

# Part 3: Indexing and Optimization

This part of the project focuses on improving the performance of your databases, both relational and NoSQL. You will learn how to use indexes and consider query rewriting to improve query performance and how to do schema and data model optimization. You will learn how to evaluate the impact of these changes in the performance of your database.

  1. Rewrite the queries developed in part 1 and 2 in case they can be optimized.
  2. Apply indexes to both your databases (relational and NoSQL) to improve the performance of your complex operations implemented in parts 1 and 2.
  3. Introduce changes to the relational schema to improve the performance
  4. Consider alterations to the data model in NoSQL to improve the performance
  5. Demonstrate the impact of the options 1-4 in each query performance using the analytical tools provided by each database system.
  6. Discuss the trade-offs (if any) between each design choice for each query.



