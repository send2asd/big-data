# Ajay Singh
## Master of Science in Computer Science
### Overview
- This assignment involves using Apache Hive, a data warehousing and SQL-like query language that simplifies the processing and analysis of structured data in Hadoop. The provided data set, "median_income_by_zipcode_census_2000," will be analyzed using Hive queries to answer specific questions and perform insightful queries.

## Setup
- Ensure your reflection is properly set up using the provided Docker image and the BDE 2020 Apache Hive setup.
Clone the repository located at docker-hive.
Start the Hadoop cluster using Apache Hive with docker-compose up -d.
Mount your local directory to the NameNode by adding a volume in the compose file.
Log into the NameNode with docker exec -it containerName bash (Replace containerName with the actual container ID or name from docker ps).
Connect to the cluster with the command docker-compose exec hive-server bash.
Further, connect to the cluster using the command /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000.

## Hive Tasks
### Task-1: Database and Table Creation 
Open a terminal/command window to log into the NameNode and create a directory named "/hive_data".
Upload the data from the local directory to the HDFS directory you created.
Open another terminal/command window and start running Hive.
Create a database in Hive named "CorseCodeHIVE" and use this database.
Create a table named "Assignment04" and load the data in HDFS to this table.

### Task-2: Hive Query #1 (20 Points)
- Write a Hive query that results in two fields - zip code and income - with incomes below $50,000 (excluding $50,000).
Store results in HDFS /result/Hive_Query_01.out.

### Task-3: Hive Query #2 (20 Points)
- Write a Hive query to output three fields - id, zip code, and income - sorted in ascending order by income, containing records with income strictly larger than the average income across all records.
Store results in HDFS /result/Hive_Query_02.out.

### Task-4: Hive Query #3 (20 Points)
- Develop your own useful, interesting, and insightful query (just one query) and results.
Store results in HDFS /result/Hive_Query_03.out.
## Questions:  
- List all commands and queries you used in HIVE.
  
### Task 1 
Creating the database:
```
CREATE DATABASE CS6500HIVE;
```  
Creating a table before loading the data:
```
create table Assignment04(id string, zip string, geo1 string, geo2 string, salary int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ LINES TERMINATED BY ‘\n’ tblproperties ("skip.header.line.count"="2");

```  
Loading the data:
```
LOAD DATA INPATH '/hive_data/DEC_00_SF3_P077_with_ann.csv' OVERWRITE INTO TABLE Assignment04;
``` 
### Task 2
Selecting zipcode and salary from the table which have salary < 50000.
```
INSERT OVERWRITE DIRECTORY "/result/Hive_Query_01.out" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  
SELECT zip,salary FROM Assignment04 WHERE salary<50000;
```
### Task 3

Selecting id, zip code and income in ascending order of income having income more than the average income.
```
INSERT OVERWRITE DIRECTORY "/result/Hive_Query_02.out" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
 SELECT t.id,t.zip,t.salary FROM (SELECT id,zip, salary, AVG(salary) OVER()  AS avgSal FROM Assignment04 ) t WHERE salary > avgSal order by salary ASC;

```  
  
### Task 4

I have selected all the detail for employee which has highest salary.
```
INSERT OVERWRITE DIRECTORY "/result/Hive_Query_03.out" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
 SELECT * FROM Assignment04 WHERE salary IN(select Max(salary) from Assignment04);

```   
#### Three output files in three query
- Output files are uploaded with their respective name which mention in queries.
- Please ignore the Hive_Query_01.out directory. All output files are at the same level of Readme.md

#### What technical errors did you experience?
- Presto-cordinator 1 getting failed during docker up with the error 'runtime: failed to create new OS thread (have 2 already; errno=22)'.

#### What conceptual difficulties did you experience?
- Creating tables in hive. 

#### How much time did you spend on each part of the assignment?
 - Gitlab & Git: 1 hour
 - Docker setup/usage: 4-5 hours
 - Actual reflection work: 3-4 hours

#### What was the hardest part of this assignment?
- The setup of of the new container in docker was hardest because it was getting failed in mac.
