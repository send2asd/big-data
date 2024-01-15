Family = LOAD 'DEC_00_SF3_P077_with_ann.csv' USING  org.apache.pig.piggybank.storage.CSVExcelStorage() as ( Id:chararray, Zip:chararray, Geography:chararray, Family_Income:int);
Family_Less_Income = FILTER Family  BY Family_Income < 50000;
Result = foreach Family_Less_Income generate Zip, Family_Income;
STORE Result INTO 'hdfs://localhost:9000/Pig_task_1.out' USING PigStorage (',');