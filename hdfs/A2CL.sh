#!/bin/sh
echo "1. Creating directory dict1 and dict2"
hdfs dfs -mkdir /dict1/
hdfs dfs -mkdir /dict1/question/
hdfs dfs -mkdir /dict1/answer/
hdfs dfs -mkdir /dict2/
hdfs dfs -mkdir /dict2/question/
echo "2. Adding the A2_Question.txt into HDFS as the path /dict1/question/"
hdfs dfs -put A2_Question.txt /dict1/question/ 
echo "3. Listing  the content of dir /dict1/question/"
hdfs dfs -ls /dict1/question/
echo "4. Reading content of question file from directory /dict1/question/"
hdfs dfs -cat  /dict1/question/A2_Question.txt
echo "5. Moving HDFS file /dict1/question/A2_Question.txt to /dict2/question/ and renaming it as A2_QuestionCopy.txt"
hdfs dfs -mv /dict1/question/A2_Question.txt /dict2/question/A2_QuestionCopy.txt
echo "6. Putting the A2_Answer.txt into HDFS as the path: /dict1/answer/"
hdfs dfs -put A2_Answer.txt /dict1/answer/ 
echo "7. Reading the contents of A2_Answer.txt"
hdfs dfs -cat  /dict1/answer/A2_Answer.txt
echo "8. Listing the disk space used by the HDFS directories /dict1/answer/ and /dict1/question/"
hdfs dfs -du /dict1/answer/
hdfs dfs -du /dict1/question/
echo "9. Recursively listing the contents of the HDFS directories /dict1/"
hdfs dfs -ls -R /dict1/
echo "10. Removing all files/directories underneath the HDFS directories named /dict1/ and /dict2/"
hdfs dfs -rm -r /dict1/
hdfs dfs -rm -r /dict2/
