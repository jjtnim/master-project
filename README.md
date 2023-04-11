# Flow Comparision for event-based data
## Requirement
- Python 3.8.12
- Pyspark 3.2.0
- Tableau 2022.4
- Tabpy Extension : [Tabpy Installtion](https://tableau.github.io/TabPy/docs/server-install.html)



## Input Data
- file format: csv
- required columns:
  - id: unique id for each sample
  - event: occured event
  - start_time: event start timestamp
  - end_time: event end timestamp
  
 ## Step
 - Step 1: using data_transformation.py to export need data
 - Step 2: connect data to Tableau File
