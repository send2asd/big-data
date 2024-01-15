Family = LOAD 'DEC_00_SF3_P077_with_ann.csv' USING  org.apache.pig.piggybank.storage.CSVExcelStorage() as ( Id:chararray, Zip:chararray, Geography:chararray, Family_Income:int);
Family_GROUP = GROUP Family ALL;
AVG_INCOME = FOREACH Family_GROUP GENERATE AVG(Family.Family_Income) as avg;
UNORDER_INCOME = FILTER Family  BY Family_Income > AVG_INCOME.avg;
ORDER_INCOME = Order UNORDER_INCOME  BY Family_Income ASC;
Family_Detail = FOREACH ORDER_INCOME generate Id, Zip, Family_Income;
STORE Family_Detail INTO 'hdfs://localhost:9000/Pig_Task_2.out' USING PigStorage (',');