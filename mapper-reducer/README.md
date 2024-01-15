# Ajay Singh
## Master of Science in Computer Science
## Overview
- This project focuses on implementing MapReduce algorithms using Apache Hadoop and the Python mrjob library. The tasks involve exploring and filtering random text files to calculate average values and apply specific filters. The project is divided into two main tasks.
### What commands did you use to run each script?
``` 
    python3 FirstJob.py randomBook.txt > FirstJob_OutPut.txt
    python3 SecondJob.py randomBook.txt > SecondJob_OutPut.txt
    python3 ThirdJob.py randomBook.txt > ThirdJob_OutPut.txt
    python3 FourthJob.py randomBook.txt > FourthJob_OutPut.txt
    python3 OneStepMean.py HT_Sensor_dataset.dat > OneStepMean_OutPut.txt

```
#### What technical errors did you experience?
- Python print function was not working.
- In Task 2: string header created problem in mapper function. Which handled by additional check.

#### What conceptual difficulties did you experience?
- OneStepMean: writing code for this was difficult.
- How mrjob interacts with Hadoop.

### What was the hardest part of this assignment?
-  OneStepMean was the hardest part.

### What was the easiest part of this assignment?
- FirstJob and SecondJob was easiest.
