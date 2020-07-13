# Data lake

---



### Introduction


- A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.




- As data engineer, I building an ETL pipeline that extracts their data from S3, processes them using Koalas, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

This provides an environment that is designed for:

* Capability of a data system, network, or process to handle a growing amount of data or its potential to be enlarged in order to accommodate that data growth. 
* Store the multi structured data from diverse set of sources.
* the data lake can able to leverage both structured and unstructured data. 
and more .. 



### Database schema design and ETL process:


* I modeled the database using the Star Schema Model by used python ,Koalas and Spark Engine.We have got one Fact table, "songplays" along with four more Dimension tables named "users", "songs", "artists" and "time".


* First, I used etl.ipynb notebook to perform ETL on the local enviroment, by used `song_data` and `log_data` , to load data from S3 and test etl pipline.


* Then, used etl.py to load data from s3 to analytics tables on AWS the `artists`,`time` and `users` dimensional tables, as well as the `songplays` fact table.





### Files in repository

Description of files and how to use them in your own application are mentioned below.

| File | Description |
| ------ | ------ |
| dl.cfg |Contain of AWS info |
| etl.ipynb |This notebook contains detailed instructions on the ETL process for each of the tables.|
| etl.py | is where you'll load data from S3 into tables on s3.|
| Others |partitiond data due implement ETL pipeline in local enviroment.|





