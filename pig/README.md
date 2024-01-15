# Ajay Singh
## Master of Science in Computer Science
## Overview
- This assignment involves using Pig, a high-level platform for creating MapReduce programs to simplify the data processing tasks in Hadoop. The provided data set, "median_income_by_zipcode_census_2000," will be analyzed using Pig scripts to answer specific questions and perform insightful queries.

## Setup
- Ensure your assignment is properly set up using the provided Docker image.
- Use the following command to run the Docker image and start Pig in local mode:

```docker run -it -p 50070:50070 -v "$PWD":/mySpace -p 8088:8088 -p 8080:8080 suhothayan/hadoop-spark-pig-hive:2.9.2 bash```

### Start Pig in local interactive mode: pig -x local

## Pig Tasks
### Pig Task #1:
- Write a Pig script that results in two fields - zip code and income - with incomes below $50,000 (excluding $50,000).
Store results in Pig_Task_1.out.

### Pig Task #2:

- Write a Pig script to output three fields - id, zip code, and income - sorted in ascending order by income, containing records with income strictly larger than the average income across all records.
Store results in Pig_Task_2.out.
### Pig Task #3:
- Develop a useful, interesting, and insightful query using Pig.
Store results in Pig_Task_3.out.

### Pig Latin Script for Task 1(Part1.pig)
``` 
Family = LOAD 'DEC_00_SF3_P077_with_ann.csv' USING  org.apache.pig.piggybank.storage.CSVExcelStorage() as ( Id:chararray, Zip:chararray, Geography:chararray, Family_Income:int);
Family_Less_Income = FILTER Family  BY Family_Income < 50000;
Result = foreach Family_Less_Income generate Zip, Family_Income;
STORE Result INTO 'hdfs://localhost:9000/Pig_Task_1.out' USING PigStorage (',');

```

### Pig Latin Script for Task 2(Part2.pig)
``` 
Family = LOAD 'DEC_00_SF3_P077_with_ann.csv' USING  org.apache.pig.piggybank.storage.CSVExcelStorage() as ( Id:chararray, Zip:chararray, Geography:chararray, Family_Income:int);
Family_GROUP = GROUP Family ALL;
AVG_INCOME = FOREACH Family_GROUP GENERATE AVG(Family.Family_Income) as avg;
UNORDER_INCOME = FILTER Family  BY Family_Income > AVG_INCOME.avg;
ORDER_INCOME = Order UNORDER_INCOME  BY Family_Income ASC;
Family_Detail = FOREACH ORDER_INCOME generate Id, Zip, Family_Income;
STORE Family_Detail INTO 'hdfs://localhost:9000/Pig_Task_2.out' USING PigStorage (',');

```



### Pig Latin Script for Task 3(Part3.pig). In this task counting the records for those whose family income is greater than 100000.
``` 
Family = LOAD 'DEC_00_SF3_P077_with_ann.csv' USING  org.apache.pig.piggybank.storage.CSVExcelStorage() as ( Id:chararray, Zip:chararray, Geography:chararray, Income:int);
Six_digit_salary = FILTER Family  BY Income > 100000;
Group_data = GROUP Six_digit_salary ALL;
Six_digit_salary_count = foreach Group_data  Generate COUNT(Six_digit_salary);
STORE Six_digit_salary_count INTO 'hdfs://localhost:9000/Pig_Task_3.out' USING PigStorage (',');

```


#### What technical errors did you experience?
- DataNode getting failed while running Pig files in local mode.
- In Task 3: Pig latin has some limitation in query. Unable to find any function which can directly calculate length of any integer value in Pig Latin.

#### What conceptual difficulties did you experience?
- Downloading ouput file from HDFS to local system. 

#### What advice would you give someone doing this assignment in the future?
- They should run Pig Script also in MapReduce mode along with Local mode for more learning.
