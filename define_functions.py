# Import needed libraries
from pyspark.sql import DataFrame
from json import dumps

def create_dataframe_from_dictionary(dictionary: dict) -> DataFrame:
    """This function takes a dictionary as an input (usually from a JSON). It creates a dataframe out of the dictionary by continuously exploding its nested components. The resulting dataframe's schema consists of the dictionary's first set of keys, in addition to any key in a nested dictionary within. """
    # Step 1: Convert the dictionary into a JSON formatted string
    json_string = json.dumps(dictionary)
    # Step 2: Using Spark Context's parallelize method to allow for parallelization
    jsonRDD = sc.parallelize([json_string])
    # Step 3: Create a dataframe
    df = spark.read.json(jsonRDD)
    # Step 4: Identify array columns to prepare for row explosion
    array_cols = get_column_list_of_data_type(df, ArrayType)
    # Step 5: Explode the array columns
    for array_col in array_cols:
        df = df.withColumn(array_col + "_exploded", explode(col(array_col)))
        # Check if there are more array columns, and if so reiterate the loop for those
        array_cols = get_column_list_of_data_type(df, ArrayType)
    return df

def get_column_list_of_data_type(dataframe: DataFrame, data_type) -> list:
    """This function takes a dataframe and a data type as inputs. It outputs a list of column names where the data type for that column is the specified one."""
    try:
        return [
            col.name
            for col in dataframe.schema
            if isinstance(col.dataType, data_type)
        ]
    except TypeError:
        print(f"The specified type {data_type} is not valid.")
        return []
    
 def join_on_closest_time_match(
        primary_dataframe: DataFrame,
        secondary_dataframe: DataFrame,
        primary_time_column: str,
        secondary_time_column: str,
        additional_join_columns=[],
        max_seconds_difference_tolerance: int = 3600,
    ) -> DataFrame:

        """This function takes two dataframes containing date columns. It joins them based on the closest timestamp match, such that the difference between the two matched timestamps is at most a certain value (given by the max_time_difference_tolerance parameter).
        primary_dataframe: DataFrame (The dataframe containing the original timestamp column)
        secondary_dataframe: DataFrame (The dataframe containing timestamp column)
        primary_time_column: str (A string representing the name of the primary_dataframe's timestamp column)
        secondary_time_column: str (A string representing the name of the secondary_dataframe's timestamp column)
        additional_join_columns: list (A list of strings representing columns to join the dataframes on, besides the closest timestamp match)
        max_seconds_difference_tolerance: int (An integer representing the threshold at which two timestamps can be considered sufficiently close)
        
        Example usage:
        primary_dataframe = join_on_closest_time_match(primary_dataframe, secondary_dataframe, "Timestamp", "Timestamp_secondary", ["CategoryCode", "GroupId"], 2*60*60);
        """

        # Step 0: Check that the input data has expected format
        condition1 = (
            primary_dataframe.select(primary_time_column).dtypes[0][1] == "timestamp"
        )
        condition2 = (
            secondary_dataframe.select(secondary_time_column).dtypes[0][1]
            == "timestamp"
        )
        if (condition1 and condition2) == False:
            print(
                "Datatype of the relevant timecolumns in the passed in DataFrames is not 'timestamp'"
            )  # This should be error logging
            return None

        # Step 1: Select needed columns
        primary_dataframe_selection = primary_dataframe.select(
            primary_time_column, *additional_join_columns
        ).dropDuplicates()
        secondary_dataframe_selection = secondary_dataframe.select(
            secondary_time_column, *additional_join_columns
        ).dropDuplicates()

        # Step 2: Join on additional column if specified. Otherwise, do a cross join
        if len(additional_join_columns) != 0:
            print(f"Joining the tables on {additional_join_columns}")
            helper = primary_dataframe_selection.join(
                secondary_dataframe_selection, additional_join_columns, how="left"
            ).dropDuplicates()
        else:
            print(
                f"No specified join columns. Cross join between tables of row count {primary_dataframe_selection.count()} and {secondary_dataframe_selection.count()} will be implemented."
            )
            helper = primary_dataframe_selection.crossJoin(
                secondary_dataframe_selection
            )

        print(f"The size of the first joined dataframe is {helper.count()}")
        # Step 3: Calculate the difference in time between the two dates
        helper = helper.withColumn(
            "TimeDifference",
            abs(
                col(primary_time_column).cast("float")
                - col(secondary_time_column).cast("float")
            ),
        )

        # Step 4: Remove matched rows where the time difference is greater than tolerated level
        helper = helper.filter(
            col("TimeDifference") <= max_seconds_difference_tolerance
        )

        # Step 5: Sort the dataframe in ascending order based on the difference in time, i.e. the "first" record is the one closest to the time in primary dataframe (If two timestamps are equally distant, we are then sorting by that column in ascending order to always pick the "older" timestamp)
        helper = helper.sort(
            *additional_join_columns,
            col(primary_time_column),
            col("TimeDifference").asc(),
            col(secondary_time_column).asc(),
        )

        # Step 6: Select the closest time
        helper = helper.groupby(*additional_join_columns, primary_time_column).agg(
            first(col(secondary_time_column)).alias(secondary_time_column),
            first(col("TimeDifference")).alias("TimeDifference"),
        )

        # Step 7: Join with primary dataframe
        primary_dataframe = primary_dataframe.join(
            helper, [*additional_join_columns, primary_time_column], how="left"
        )

        # Step 8: Join with secondary dataframe
        primary_dataframe = primary_dataframe.join(
            secondary_dataframe,
            [*additional_join_columns, secondary_time_column],
            how="left",
        ).drop(secondary_time_column, "TimeDifference")

        return primary_dataframe
