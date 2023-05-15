# Please create a script / environment w/ spark or pandas 
# This script should read using the provided datasets & outputs a file for each question. 

# There are 3 tables to note here. 
# groups - a collection of group information related to profiles
# profiles - a collection of profiles # Note a profile is only valid if it has a externalid
# unsubscribe - a list of emails


u = spark.read.csv(path="/root/data/data/unsubscribe.csv")
u.createOrReplaceTempView("u")
p = spark.read.json("/root/data/data/profile.json")
p.createOrReplaceTempView("p")
g = spark.read.parquet("/root/data/data/group.parquet")
g.createOrReplaceTempView('g')

# Question 1
# create a breakdown of profiles by gender
# a field should be created for each of the following fields
# total verified profiles
# total female profiles
# total male profiles
# total unidentified profiles

profiles = spark.read.json("/root/data/data/profile.json")
profiles.createOrReplaceTempView("profiles")

filtered_profiles = spark.sql("select distinct profile_id, email, externalid, first_name, gender, last_name from profiles where externalid is not null")
filtered_profiles.createOrReplaceTempView('fp')

spark.sql("""
    select 
        count(CASE WHEN UPPER(gender) = 'FEMALE' THEN 1 END) as female_count,
        count(CASE WHEN UPPER(gender) = 'MALE' THEN 1 END) as male_count,
        count(CASE WHEN (UPPER(gender) not in ('FEMALE', 'MALE') or gender is Null) THEN 1 END) as unidentified_count,
        count(1) as total
    from fp
""").show()

# +------------+----------+------------------+-----+
# |female_count|male_count|unidentified_count|total|
# +------------+----------+------------------+-----+
# |         108|       124|               617|  849|
# +------------+----------+------------------+-----+

#### should note that there is 1 record that is unidentified & 

#  975 contains two records w/ gender null & female
#  22 contains a value with female instead of Female 
#  212 is duplicate role
#  313 is duplicate with different names


# Question 2 
# For each group in GBP acquire the following
# group name
# total admins
# total total group count
# admins to users percentage to 2 decimal points

profiles = spark.read.json("/root/data/data/profile.json")
profiles.createOrReplaceTempView("profiles")

groups = spark.read.parquet("/root/data/data/group.parquet")
groups.createOrReplaceTempView('groups')

filtered_profiles = spark.sql("select distinct profile_id, email, externalid, first_name, gender, last_name from profiles where externalid is not null")
filtered_profiles.createOrReplaceTempView('filtered_profiles')

admins = spark.sql("""
        select 
            g.group_name, 
            count(CASE WHEN is_admin is true THEN 1 END) as admin_count,
            count(distinct p.profile_id) as group_count, 
            (count(CASE WHEN is_admin is true THEN 1 END)/count(distinct p.profile_id)) * 100 as admin_to_user_rate
        from filtered_profiles as p 
        join groups as g 
        on p.profile_id = g.profile_id 
        where p.externalid is not null 
        and country_code='GBP' 
        group by g.group_name
""")
admins.createOrReplaceTempView("admins")

spark.sql("""
select 
group_name, admin_count, group_count, CAST(admin_to_user_rate as DECIMAL(4,2)) as admin_to_user_rate
from admins
""").show()

# +------------+-----------+-----------+------------------+
# |  group_name|admin_count|group_count|admin_to_user_rate|
# +------------+-----------+-----------+------------------+
# |     Moto GP|          7|         86|              8.14|
# |Running Club|          9|         77|             11.69|
# | Tennis Club|         10|         82|             12.20|
# +------------+-----------+-----------+------------------+
### value should be stored as decimal 4,2


# question 3
# create an email list of admins from the USA from Running Club & Tennis club
# please exclude any emails who have unsubscribed

profiles = spark.read.json("/root/data/data/profile.json")
profiles.createOrReplaceTempView("profiles")

groups = spark.read.parquet("/root/data/data/group.parquet")
groups.createOrReplaceTempView('groups')

filtered_profiles = spark.sql("select distinct profile_id, email, externalid, first_name, gender, last_name from profiles where externalid is not null")
filtered_profiles.createOrReplaceTempView('filtered_profiles')

unsub = spark.read.csv(path="/root/data/data/unsubscribe.csv")
unsub.createOrReplaceTempView("unsub")
unsub = spark.sql('select _c0 as email from unsub')
unsub.createOrReplaceTempView("unsub")


admins = spark.sql("""
        select 
            p.email
        from filtered_profiles as p 
        join groups as g 
        on p.profile_id = g.profile_id 
        left join unsub u 
        on p.email = u.email
        where g.is_admin is true 
        and p.externalid is not null 
        and country_code='USA' 
        and group_name in ('Running Club', 'Tennis Club')
        and u.email is null
""").show()
## total of 25