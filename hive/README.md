# Ajay Singh
## Master of Science in Computer Science

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
