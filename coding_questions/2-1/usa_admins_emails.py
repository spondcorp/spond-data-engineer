import logging
import os
import sys

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from coding_questions.utils import get_spark_session


def retrieve_corrupt_records(df: DataFrame) -> DataFrame:
    """
    Retrieve the corrupt records from the parquet file.
    This function caters for a specific type of corrupt file where removing the first and last characters would result
    in a valid JSON string that maps to the dataset fully.
    """
    corrupt_record_col_name = "_corrupt_record"
    corrupt_record_col = F.col(corrupt_record_col_name)

    schema = df.drop(corrupt_record_col_name).schema

    # Tidy the corrupt records so the string become a valid JSON
    clean_record = F.when(
        corrupt_record_col.isNotNull(),
        corrupt_record_col.substr(F.lit(2), F.length(corrupt_record_col) - F.lit(2)),
    ).otherwise(
        corrupt_record_col,
    )

    # Convert the corrupt record string into a Spark Struct
    fixed_record = F.when(
        corrupt_record_col.isNotNull(),
        F.from_json(clean_record, schema),
    ).otherwise(
        F.lit(None),
    )

    # Add the recovered values to their respective columns
    for col in schema.names:
        df = df.withColumn(
            col,
            F.when(
                (F.col(col).isNull()) & (corrupt_record_col.isNotNull()),
                fixed_record[col],
            ).otherwise(F.col(col)),
        )

    return df


def read_groups_data(path: str = sys.argv[0] + os.sep + "../../../datalake/group") -> DataFrame:
    """
    Read the groups dataset from a given path, fix the corrupt record, and return a DataFrame with select columns.
    """
    logging.info("Reading groups data")
    # Parquet files are self-describing so there's no need to define the schema on read as it will be preserved.
    # Some checks could be done on the columns to check for quality such as case and spacing but the data is clean, so
    # I will work on that assumption
    return retrieve_corrupt_records(spark.read.parquet(path)).select(
        # Only select the columns that are needed in order to reduce the amount of data being moved around
        "profile_id",
        "country_code",
        "group_name",
    )


def read_profiles_data(path: str = sys.argv[0] + os.sep + "../../../datalake/group") -> DataFrame:
    """
    Read the profiles dataset from a given path and return a DataFrame with select columns.
    """
    logging.info("Reading profiles data")
    profiles_schema = T.StructType(
        [
            # Only select the columns that are needed in order to reduce the amount of data being moved around
            T.StructField("email", T.StringType(), True),
            T.StructField("profile_id", T.IntegerType(), True),
        ],
    )

    # The profiles dataset contains duplicate emails, however that doesn't affect the output of this pipeline.
    # It's more computationally expensive to fix it rather than just process it and remove the side effects at the end
    # If I were to fix it, I would group on profile_id and select the rows with the latest created_at value
    # If there were still duplicates then, there would be a few options
    # 1. Keep the row with the least amount of nulls (assuming the data wasn't requested to be removed by the user)
    # 2. Keep the row where the most critical values are present based on business need.
    #    For example, a non-null email might be more useful than a non-null gender to allow communication with the user
    # 3. Combine the two approaches above to build one row with as much information as available
    return spark.read.option("multiline", "true").json(path, schema=profiles_schema)


def read_unsubscribe_data(path: str = sys.argv[0] + os.sep + "../../../datalake/unsubscribe") -> DataFrame:
    """
    Read the unsubscribe dataset from a given path, remove invalid emails, and return a DataFrame.
    """
    logging.info("Reading unsubscribe data")
    unsubscribe_schema = T.StructType(
        [
            T.StructField("email", T.StringType(), True),
        ],
    )

    # This dataset contains some bad data that needs to be removed so the produced emails are valid.
    # There are two options to remove them
    # 1. Run the validation on the full dataset before processing it further
    #    Pros: Reduces the amount of rows used in the join which will also reduce shuffling during the join.
    #    Cons: Regex is expensive and this way it runs on all rows even the ones that will be excluded during the join
    # 2. Run the validation on the output dataset after the join is done
    #    Pros: Regex is expensive and this way it runs only on the relevant
    #    Cons: Shuffling is expensive and this way the amount of data is larger than the first approach
    # Deciding on which approach to take depends on the size of the two datasets and their data quality
    # In this case the size is too small to make a noticeable difference either way. However, I am considering
    # a much larger scale of data
    df = spark.read.csv(path, schema=unsubscribe_schema)

    email_regex = r"([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+"

    email_is_valid = F.col("email").rlike(email_regex) & F.col("email").isNotNull()

    unsubscribe_invalid_df = df.where(~email_is_valid)
    logging.warning(f"Dropping {unsubscribe_invalid_df.count()} row(s) with invalid email(s)")

    return df.where(email_is_valid)


if __name__ == "__main__":
    spark = get_spark_session()
    groups_df = read_groups_data()
    profiles_df = read_profiles_data()
    unsubscribe_df = read_unsubscribe_data()

    usa_running_and_tennis_groups_df = groups_df.where(F.col("country_code") == "USA").where(
        F.col("group_name").isin("Running Club", "Tennis Club"),
    )

    # Use the groups dataset as the base and get all the emails from the profiles dataset using a left join
    # Use an exclusive/anti join to eliminate the rows with unsubscribed emails from the result of the first join
    email_list_df = (
        usa_running_and_tennis_groups_df.alias("groups")
        .join(
            profiles_df.alias("profiles"),
            F.col("groups.profile_id").eqNullSafe(F.col("profiles.profile_id")),
            "left",
        )
        .alias("base")
        .join(
            unsubscribe_df.alias("unsubscribe"),
            F.col("base.email").eqNullSafe(F.col("unsubscribe.email")),
            "left_anti",
        )
    )

    invalid_emails_df = email_list_df.where(F.col("email").isNull())
    logging.warning(f"Found {invalid_emails_df.count()} null email(s) for group admins)")

    valid_emails_df = email_list_df.where(F.col("email").isNotNull()).distinct()
    logging.info("Writing list of admin emails")
