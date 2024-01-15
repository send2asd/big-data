# Ajay Singh
## Master of Science in Computer Science

### Overview
- This project involves utilizing Apache Spark for data analysis tasks. The tasks include working with stock data, converting a Jupyter Notebook into a Python script, and performing various analyses on a dataset containing information about products, sellers, and sales. The project aims to demonstrate proficiency in Spark and data manipulation within a Jupyter environment.

### Setup
- Ensure your environment is set up according to the provided instructions and Docker configuration.
Use the Jupyter Docker Stacks to create a Jupyter Notebook for Spark interaction.
Mount the required directory using the command: docker run --rm -d -p 8888:8888 -v "$PWD":/home/jovyan/work jupyter/all-spark-notebook.


### Task-1: Stock Data Analysis
- Replace the stock data with Stock data from SP_500_Historical.csv.
Recreate the Jupyter Notebook following the instructions in the provided tutorial video.
Convert the Jupyter Notebook into a single Python script for Spark submission. The script should read the CSV file from HDFS.
Result: Jupyter Notebook, Python Script.

### Task 2-6: Sales Data Analysis
- Download the dataset from Google Drive.
Perform the following tasks in a Jupyter notebook using Spark:
- Task-2: Determine the distinct products sold each day.
- Task-3: Calculate the average revenue of the orders.
- Task-4: Find the average daily revenue of each product.
- Task-5: Calculate the average % contribution of an order to each seller's daily quota.
- Task-6: Identify the second most and least selling sellers for each product.
- Result: Jupyter Notebook with complete codes and results.
  
### What are the advantages and disadvantages of Column-based storage? Explain these in plain English. 

**Advantages:**
1. Columnar storage improves query performance by reducing the amount of data that needs to be read from disk. With columnar storage, only the columns that are needed for a query are read from disk, reducing disk I/O and improving query performance.

2. In Columnar storage more data can be stored in a given amount of disk space, which reduce storage cast.

3. Columnar storage is well-suited for parallel processing, which is important for distributed computing platforms like Spark. Since data is stored in columns, parallel processing can be performed on individual columns, improving overall processing speed.


**Disadvantages:**
1. It is not as efficient for insert and update operations as row-based storage. This is because columnar storage requires more processing overhead to maintain data integrity.

2. Limited support for complex data types.

3. Columnar storage may require additional indexing structures to support efficient querying, which can add complexity and overhead to the storage system.

- In summary, columnar storage in Spark provides many advantages, especially for read-oriented workloads, but also has some disadvantages to consider when choosing a storage format.


### What technical errors did you experience? Please list them explicitly and identify how you corrected them.
- I phased error on importing findspark packages. 

### What conceptual difficulties did you experience?
- On Task 4 query for average daily revenue took some brainstorming. 
  
### What was the hardest part of this assignment?
- Running jupyter notebook with spark was difficult. Also writing query in spark is difficult as I was writing this first time.

### What was the easiest part of this assignment?
- Task 1 was easy.

### What advice would you give someone doing this assignment in the future?
- Do not run anaconda jupyter if you are running jupyter from inside docker. It cretes conflict. 
