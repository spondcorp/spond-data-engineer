# Conceptual questions
The following tasks are meant to test your ability to design an architecture for analytics and machine learning applications. Please be as specific as possible in your responses and elaborate on your decisions. Include any illustrations (like high-level architecture diagram) that you feel will support your answer. No coding is required in this section.

Spond has multiple sources of data that pertain to different areas of the business. Generally speaking, the three main categories are 1) internal application data, 2) external vendor data, and 3) other third party data.

1. The internal application data can be assumed to be stored at a distributed set of RDS instances in AWS. This data mainly relates to data generated from the applications that Spond provides to end-users. Examples of datasets include payments, profiles, groups, clubs, etc.

2. External vendor data are data that are stored with external service providers that relate to different business areas in Spond. Some are user-facing (e.g., Intercom for product support and Typeform for user surveys), while others are used as internal tooling (e.g., CRM systems, accounting systems, etc.). The format and availability of the data vary by vendor.

3. Other third party data primarily concern datasets that are needed for intermediate enrichments of existing data. Examples include foreign exchange rates, geolocation mapping, public demographics data, etc. The format and availability of the data vary by vendor - but are mostly API based.


## Question 1.1 - Data ingestion
Your goal is to design a robust set of data ingestion pipelines with the purpose of extracting data from the source system. Please detail the technology stack and tools you would utilize to extract and ingest data from these diverse sources into our system. In your response, describe your strategy for handling the different data formats and availability across these sources. Additionally, elaborate on your approach to optimizing this data ingestion process in terms of both performance and cost. Consider aspects such as handling data in real-time versus batch processing, dealing with large data volumes, and potential network issues.

## Question 1.2 - Data transformation and cleaning
Once the data is ingested and stored, it's often required to transform and clean the data to make it suitable for downstream applications like analytics and machine learning. Describe your approach to data transformations and cleaning. What tools or frameworks would you use for this task? How would you handle schema evolution and ensure data quality? Please give examples of some common data quality issues and how you would address them.


# 2. Coding questions
Please create a script/environment with Spark or pandas.

This script should read using the provided datasets and outputs a file for each question. Our recommendation is to create a docker environment to run a spark script and to verify operation. It is important for the ability to replicate the environment on any machine. Please state any assumptions and thought process as much as possible within comments.

There are 3 tables to note here.

1. `groups` - a collection of group information related to profiles
2. `profiles` - a collection of profiles # Note a profile is only valid if it has a externalid
3. `unsubscribe` - a list of emails

## Question 2.1
Create an email list of admins from the USA from Running Club & Tennis club.please exclude any emails who have unsubscribed.

## Question 2.2
For this question, please use the two tables found in the profiles_history folder. Note that this task does not depend on answers to any of the previous questions.

- profiles_history.json: Existing profiles in the data warehouse at time t=0
- new_profiles.json: Profiles in the database at time t=1

Note that the profiles_history.json dataset includes two columns not present in the profiles.json dataset used in the previous questions.

- `valid_from`: the date at which the record was originally inserted into the table.
- `valid_to`: the last day the record was considered active - active records are set to 2099-12-31.

Please perform a merge of the two datasets with the following requirements:

1. Ten records have experienced changes in their email_address property between time t=0 and t=1. The consolidated table should maintain the original email_address. For instance, if the email for profile 1 altered from `abc@xyz.com` to `def@xyz.com` within the time frame t=0 to t=1, both of these records need to be included in the consolidated dataset.
2. There are ten records that were removed between the times t=0 and t=1. It's crucial to keep these records in the consolidated dataset.

Please shortly comment on your thought process, and explain how the valid_from or valid_to timestamps were modified.

Finally, please output a list of the profile_ids that were updated (1) and deleted (2) in addition to the merged table.


# How do deliver results

You can send in your answers a link to a public repository.
