# FINALE_PROJECT_BIG_DATA
## Flight Delays and Cancellations Analysis

### Problem Statement
- We are utilizing the 2015 Flight Delays and Cancellations dataset provided by the Department of Transport. The dataset includes information on airports, flights, and airlines. The "airport.csv" file contains details such as the name, city, longitude, and latitude of airports. The "flight.csv" file includes flight details like the day of the flight, departure and arrival times, flight code, delays, and distance between the destination and source airport. Additionally, the "airlines.csv" file provides airline data, including names and codes.

### Proposed Approach
- We will leverage PySpark, part of the Apache Spark framework, to read and process the dataset. The initial steps involve data cleaning to handle any missing or inconsistent data. Subsequently, we will perform various queries to extract insights from the dataset.

### Task 1: Rank Airlines
- We will rank flight companies based on their punctuality and airtime. This ranking will consider factors such as delays, airtime, and flight speed. Calculating flight speed will contribute to the overall ranking.

### Task 2: Find Route Between Two Airports
- We aim to determine the most optimized route between two given airports. This task involves analyzing the dataset to identify routes that minimize travel time or delays.

### Task 3: Predict Flight Delays
- Using machine learning techniques, we will train a model to predict flight delays based on historical data. This predictive model will offer insights into potential delays, allowing for proactive measures.

### Additional Tasks
- Further analysis of the dataset may reveal additional tasks and insights that can be explored. The flexibility of PySpark enables the addition of more tasks as needed.

### Setup Environment:

- Ensure you have Apache Spark and PySpark installed.
Use the provided dataset: "airport.csv," "flight.csv," and "airlines.csv."

### Execute PySpark Scripts:
- Run PySpark scripts to read, clean, and analyze the dataset.
Example command: spark-submit analyze_flights.py

### Review Results:
- Analytical results will be generated based on the defined tasks.
Explore the generated reports and insights for each task.

### Extend Analysis:
- Additional analysis tasks can be added as needed.
Modify PySpark scripts to include new queries or insights.

## Project Topics
1. **MapReduce Exploration:**
   - Compare distributed processing with traditional serial methods on a specific task.
   - Improve a traditional serial problem by implementing a MapReduce program.
   - Utilize datasets from resources like Kaggle or Google Public Datasets.

2. **Spark and SparkML:**
   - Choose a dataset and explore Spark or SparkML for tasks such as classification, regression, or detection.

3. **Tools for Big Data Education:**
   - Investigate alternatives to creating Hadoop environments with Docker containers for educational purposes.

## Collaboration
This project is a collaborative effort between two students enrolled in Big Data. Each student will provide a 1-page statement detailing their contributions and key lessons learned.

## Conclusion
This project aims to provide a hands-on experience in applying Big Data analytics techniques to real-world problems. The collaborative effort will enhance learning outcomes and foster a deeper understanding of the concepts covered in subject.

## Project structure


- Code files are inside code folder

- CSV, are inside flight_data folder


## how to run

- install pyspark and jyupter notebook
- open the data cleaning ipynb and run it 
- now run the ranking ipynb file for task1
- run path finding finding ipynb for task2
- run the machine learnin ipynb file for task 3
