# Ajay Singh
## Master of Science in Computer Science

  
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
