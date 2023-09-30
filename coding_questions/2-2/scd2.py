import datetime as dt
import logging
import os
import sys

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytz
from pyspark.sql import DataFrame

from coding_questions.utils import get_spark_session


def get_base_fields() -> list[T.StructField]:
    """
    A helper function that returns the common fields found across both datasets
    """

    return [
        T.StructField("profile_id", T.IntegerType(), True),
        T.StructField("first_name", T.StringType(), True),
        T.StructField("last_name", T.StringType(), True),
        T.StructField("email", T.StringType(), True),
        T.StructField("gender", T.StringType(), True),
        T.StructField("external_id", T.StringType(), True),
    ]


def read_new_profiles(
    path: str = sys.argv[0] + os.sep + "../../../datalake/profiles_history/new_profiles.json",
) -> DataFrame:
    """
    Read the new profiles dataset from a given path and return it as a DataFrame.
    A ´valid_from´ and ´valid_to´ column are defined to conform to the same schema as the historical dataset.
    The two columns are also set to their correct values should they be needed for row updates or creation.
    """
    logging.info("Reading new profiles data")
    return spark.read.json(
        path,
        schema=T.StructType(get_base_fields()),
    ).withColumns(
        {
            "valid_from": now_col,
            "valid_to": F.to_timestamp(F.lit("2099-12-31")),
        },
    )


def read_profile_history(
    path: str = (sys.argv[0] + os.sep + "../../../datalake/profiles_history/profiles_history.json"),
) -> DataFrame:
    """
    Read the historical profiles dataset from a given path and return it as a DataFrame
    The source dataset holds the ´valid_from´ and ´valid_to´ columns as unix timestamps of type Long; the function
     parses these columns into human-friendly values of type Timestamp
    """

    logging.info("Reading historical profiles data")

    schema = T.StructType(
        get_base_fields()
        + [
            T.StructField("valid_from", T.LongType(), True),
            T.StructField("valid_to", T.LongType(), True),
        ],
    )
    return spark.read.json(
        path,
        schema=schema,
    ).withColumns(
        {
            "valid_from": F.to_timestamp(F.col("valid_from") / F.lit(1000)),
            "valid_to": F.to_timestamp(F.col("valid_to") / F.lit(1000)),
        },
    )


def scd2_join(base_df: DataFrame, updates_df: DataFrame) -> DataFrame:
    """
    Read a base and an updates dataset and perform an SCD2 evaluation on them with the required actions stored in an
     added `action` column.
    The possible values of the `action`  column are:
    - `add`: Row exists in the updates dataset but not the base dataset
    - `update`: Row exists in both datasets but the column values are different
    - `delete`: Row exists in the base dataset but not the updates dataset
    - `noop`: Row exists in both datasets and the values are the same
    - `unknown`: None of the above or the join condition is null in both datasets
    """

    logging.info("Performing SCD2 join")
    # join the two datasets while keeping all rows from both sides to allow further comparison
    return (
        base_df.alias("base")
        .join(
            updates_df.alias("updates"),
            F.col("base.profile_id").eqNullSafe(F.col("updates.profile_id")),
            "outer",
        )
        .withColumn(
            "action",
            F.when(
                # If the row exists in the base but not the updates then the profile has been deleted
                F.col("base.profile_id").isNotNull() & F.col("updates.profile_id").isNull(),
                F.lit("delete"),
            )
            .when(
                # If the row exists in the updates but not the base then the profile has been newly created
                F.col("base.profile_id").isNull() & F.col("updates.profile_id").isNotNull(),
                F.lit("add"),
            )
            .when(
                # If the row exists in both datasets then we need to further examine it to understand the needed change
                F.col("base.profile_id").isNotNull()
                & (F.col("base.profile_id").eqNullSafe(F.col("updates.profile_id"))),
                F.when(
                    # If all the columns are equal then this is an unchanged row and no action is required
                    (
                        F.col("base.profile_id").eqNullSafe(F.col("updates.profile_id"))
                        & F.col("base.first_name").eqNullSafe(F.col("updates.first_name"))
                        & F.col("base.last_name").eqNullSafe(F.col("updates.last_name"))
                        & F.col("base.email").eqNullSafe(F.col("updates.email"))
                        & F.col("base.gender").eqNullSafe(F.col("updates.gender"))
                        & F.col("base.external_id").eqNullSafe(F.col("updates.external_id"))
                    ),
                    "noop",
                ).otherwise(
                    # If the previous condition was false then some values have changed and an update is needed
                    "update",
                ),
            )
            .otherwise(
                # This shouldn't happen unless the data is bad, but it's good practice to keep a fallback
                "unknown",
            ),
        )
    )


def execute_scd2(df: DataFrame) -> DataFrame:
    """
    Uses the output of `scd2_join` function and executes the actions in the `action` column producing a final, merged
    dataset that showing the latest state.
    """
    # For rows that require update or delete actions, the rows need to be closed by setting valid_to to the current date
    # and keeping the old values which come from the base dataset
    # For rows that are noop then they should be kept without change
    logging.info("Executing SCD2 join")
    base_df = (
        df.where(F.col("action").isin("delete", "noop", "update"))
        .select("base.*", "action")
        .withColumn(
            "valid_to",
            F.when(
                F.col("action").isin("delete", "update"),
                now_col,
            ),
        )
        .drop("action")
    )

    # For rows that require add or update actions, no further change is needed. However, the new values should be
    # preserved from the updates dataset
    updates_df = df.where(F.col("action").isin("add", "update")).select("updates.*", "action").drop("action")

    # Union both results to create the new dataset
    return base_df.unionAll(updates_df)


if __name__ == "__main__":
    spark = get_spark_session()

    # Fixing the current time so that it can be used later.
    # Using the pyspark current_timestamp function can cause mismatches as it will only be resolved when needed which
    # could potentially lead to overlapping times when a row is updated
    now = dt.datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S")
    now_col = F.to_timestamp(F.lit(now))

    joined_df = scd2_join(read_profile_history(), read_new_profiles())

    updated_ids = joined_df.where(F.col("action") == F.lit("update")).select("base.profile_id")
    deleted_ids = joined_df.where(F.col("action") == F.lit("delete")).select("base.profile_id")
    table_path = sys.argv[0] + os.sep + "../profiles"
    logging.info(f"Writing output dataset to {table_path}")
    # The `coalesce` ensures only 1 file is written out as the dataset is small. Without it, in this case, there will be
    # 2 files because the data comes from two separate partitions, one for each of the base and updates dataframes
    execute_scd2(joined_df).coalesce(1).write.mode("overwrite").format("parquet").save(table_path)
