Family = LOAD 'DEC_00_SF3_P077_with_ann.csv' USING  org.apache.pig.piggybank.storage.CSVExcelStorage() as ( Id:chararray, Zip:chararray, Geography:chararray, Income:int);
Six_digit_salary = FILTER Family  BY Income > 100000;
Group_data = GROUP Six_digit_salary ALL;
Six_digit_salary_count = foreach Group_data  Generate COUNT(Six_digit_salary);
STORE Six_digit_salary_count INTO 'hdfs://localhost:9000/Pig_task_4.out' USING PigStorage (',');