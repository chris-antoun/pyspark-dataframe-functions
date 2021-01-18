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