######### Infrastructure ########

# How would you release this project?
- CICD, Testing, Formatting, AWS / Databricks

# If the profile table were to grow dramaticaly how would you scale the job to fit the needs?
- Partitions, Parquet, Preparing Tables

# The frequency of this job eventually would like to be in realtime. Can you walk us through how you'd design this?
- where will the project live (Glue, EMR, EC2)
- what file output are you planning on using? (Avro is best for streaming)
- How do you plan on handling duplicates

# How do you plan on validating & maintaing schema?
- We use delta lake + Glue Crawler + Glue Catalog

# What has been your policy on GDPR? How have you worked to control the flow of sensitive data necessary for business operation?