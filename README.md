# Sparkify

Sparkify is a music-streaming app that wants to analyze their user event log data to
see what music users are listening to. This project creates a data pipeline with
Airflow. First, the data is extracted from S3 buckets and inserted into staging tables
on Redshift. Then the data from the staging tables are inserted into the star schema tables.
Finally, the pipeline runs data quality checks to ensure the pipeline ran correctly.