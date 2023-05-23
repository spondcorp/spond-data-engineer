# Coding Tasks
Please create a script / environment w/ spark or pandas

This script should read using the provided datasets & outputs a file for each question. Our recommendation is to create a docker environment to run a spark script & to verify operation. It is important for the ability to replicate the environment on any machine. Please state any assumptions & thought process as much as possible within your comments

There are 3 tables to note here.

1. groups - a collection of group information related to profiles
2. profiles - a collection of profiles # Note a profile is only valid if it has a externalid
3. unsubscribe - a list of emails

## Question 1
create a breakdown of profiles by gender

a field should be created for each of the following fields

- total verified profiles
- total female profiles
- total male profiles
- total unidentified profiles

## Question 2
For each group in GBP acquire the following

- group name
- total admins
- total total group count
- admins to users percentage to 2 decimal points

## Question 3
Create an email list of admins from the USA from Running Club & Tennis club.please exclude any emails who have unsubscribed.

## Question 4
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
