
<- How pipeline works ->
3 Pipeline Stages:

1) Validate data: cleaning and disk partitioning
Starts at data/0_db/... , results saved in data/1_validated/...
Disk partitioning by hour on event_time: time of event, not load_time (time data was loaded in db).
Using spark.sql.sources.partitionOverwriteMode=dynamic: (over)write only partition delta.
Data saved locally in data/, in reality this would be loaded from some unstructured source (say upstream trade system, bloomberg dump, chat, sharepoint) and saved in a DataBricks DeltaLake, S3 blob store, etc.

Linear data: (PK, NOTIONAL, EVENT_TIME, PARTIONING_TIME)
Lines (8 - 12) -> Edge cases::
- 8,8080.0,2080-09-12T00:00:00.000Z
- Date in the future
- 9,7070.0,3030387                        
- int instead of date       
- 10,1010.0,                              
- null                
- 11,1111.0,NaN                           
- not a number        
- 12,1212.0,2010-13-41T07:30:00.000Z      
- out of bounds
- 13,1313.0,01-01-2024T07:30:00.000Z      
- dd-yy-mmmm wrong format

2) Join data: joining two datasets together
Starts at data/1_validated/... , results saved in data/3_staging/...
Job runs hourly on a T-1 basis (yesterday's data). During the day new data may come in (delay in load_time).
We update only the partitions to which additional data has come in or overwrite data if needed.

Hierarchy data: (PK, FK, LOCATION)
Adjacency list describing a tree of locations. Unwraps into muliple tables.
Emulates primary keys (PK) referencing foreign keys (FK) in an imaginary relational database.

<img width="211" alt="Screenshot 2024-02-10 at 20 49 22" src="https://github.com/r-smits/hierarchy-join/assets/35615011/27a1ff90-0a6a-4be2-9c60-c93f0fcbfc14">

This enforces the excercise constraint in which a direct join does not yield all results.

Tree Assumptions:
- Shape of data stays uniform
- Non-cyclical: tree not graph
- BFS rather than DFS
- 1:1 Relationships. 1 PK references 1 FK (easiest case compatible with excercise constraint)

3) User requests from joined dataset

Starts at data/staging/... , saved in data/user/n/... (where n is some unique run id)

Daily intervals will return a single partition. More than one day all corresponding partitions.


<- Tech-stack ->

pyspark
Why: Closest to use-case. But, local cluster with one worker is not performant. On a large scale, spark outperforms alternatives. For smaller projects, pyarrow recordbatches are a better fit. Older Pandas versions have known issues with datetime fields, newer support pyarrow types.

scheduler 
Why: to schedule jobs hourly to fit excercise constraints. Easy, lightweight to set up. For larger projects: Databricks Autoload, Apache Airflow. For a single pipeline, this is overkill.

pytest
Why: Industry standard. Testing, fixtures, mocking of environment variables with monkeypatch made easy

<- Installation and run ->

- Install JVM
- pip install pipenv
- pipenv install
- export PYTHONPATH="$(pwd)"
- pipenv run start
