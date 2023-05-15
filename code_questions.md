# Please create a script / environment w/ spark or pandas 
# This script should read using the provided datasets & outputs a file for each question. 

# Our recommendation is to create a docker environment to run a spark script & to verify operation
# It is important for the ability to replicate the environment on any machine
# Please state any assumptions & thought process as much as possible within your comments

# There are 3 tables to note here. 
# groups - a collection of group information related to profiles
# profiles - a collection of profiles # Note a profile is only valid if it has a externalid
# unsubscribe - a list of emails

###################### Question 1 ######################
# create a breakdown of profiles by gender
# a field should be created for each of the following fields
# total verified profiles
# total female profiles
# total male profiles
# total unidentified profiles

###################### Question 2 ######################
# For each group in GBP acquire the following
# group name
# total admins
# total total group count
# admins to users percentage to 2 decimal points

###################### Question 3 ######################
# create an email list of admins from the USA from Running Club & Tennis club
# please exclude any emails who have unsubscribed