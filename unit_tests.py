def test__get_column_list_of_data_type(self):
    # Define input schema
    input_schema = StructType(
        [
            StructField("FirstColumn", IntegerType(), True),
            StructField("SecondColumn", ArrayType(IntegerType()), True),
            StructField("ThirdColumn", StringType(), True),
            StructField("FourthColumn", DateType(), True),
            StructField("FifthColumn", TimestampType(), True),
            StructField("SixthColumn", ArrayType(StringType()), True),
        ]
    )
    # Define input dataframe
    input_df = spark.createDataFrame(
        [
            (
                1,
                [100, 200, 300],
                "TEXT",
                date(2021, 1, 18),
                datetime(2021, 1, 18, 10, 30, 15),
                ["CODE1", "CODE2"],
            ),
        ],
        input_schema,
    )

    # Define expected output
    expected_output_1 = ["FirstColumn"]  # Case: IntegerType as an input
    expected_output_2 = ["SecondColumn", "SixthColumn"]  # Case: ArrayType as an input
    expected_output_3 = ["FourthColumn"]  # Case: DateType as an input
    expected_output_4 = ["FifthColumn"]  # Case: TimestampType as an input
    expected_output_5 = []  # Case: BooleanType as an input

    # Get output
    output_1 = get_column_list_of_data_type(input_df, IntegerType)
    output_2 = get_column_list_of_data_type(input_df, ArrayType)
    output_3 = get_column_list_of_data_type(input_df, DateType)
    output_4 = get_column_list_of_data_type(input_df, TimestampType)
    output_5 = get_column_list_of_data_type(input_df, BooleanType)

    # Get result
    expected_output = [
        expected_output_1,
        expected_output_2,
        expected_output_3,
        expected_output_4,
        expected_output_5,
    ]
    output = [output_1, output_2, output_3, output_4, output_5]
    for index in range(len(expected_output)):
        res = output[index] == expected_output[index]
        # Log test result
        )
        
def test__join_on_closest_time_match(self):
        # Define input schema for primary dataframe
        input_schema_primary_df = StructType(
            [
                StructField("Key", IntegerType(), True),
                StructField("Time", TimestampType(), True),
            ]
        )
        # Define primary input dataframe
        primary_df = spark.createDataFrame(
            [
                (1, datetime(2020, 1, 1, 3, 23, 33)),
                (1, datetime(2020, 1, 2, 16, 45, 0)),
                (1, datetime(2020, 1, 3, 23, 59, 59)),
                (1, datetime(2020, 1, 4, 8, 23, 24)),
                (2, datetime(2020, 12, 9, 3, 53, 11)),
                (2, datetime(2020, 12, 18, 9, 13, 54)),
                (2, datetime(2021, 1, 2, 15, 30, 0)),
                (3, datetime(2020, 12, 9, 3, 53, 11)),
            ],
            input_schema_primary_df,
        )

        # Define input schema for secondary dataframe
        input_schema_secondary_df = StructType(
            [
                StructField("Key", IntegerType(), True),
                StructField("Time_secondary", TimestampType(), True),
                StructField("Value", StringType(), True),
            ]
        )
        # Define secondary dataframe
        secondary_df = spark.createDataFrame(
            [
                (1, datetime(2020, 1, 1, 3, 23, 33), "FirstValue"),  # Same date match
                (
                    2,
                    datetime(2020, 12, 10, 3, 53, 11),
                    "SecondValue",
                ),  # One day difference (forward) (+24*60*60 seconds)
                (
                    2,
                    datetime(2020, 12, 8, 3, 53, 11),
                    "ThirdValue",
                ),  # One day difference (backward) (-24*60*60 seconds)
            ],
            input_schema_secondary_df,
        )

        # Define input schema for expected output dataframe
        input_schema_expected_output_df = StructType(
            [
                StructField("Key", IntegerType(), True),
                StructField("Time", TimestampType(), True),
                StructField("Value", StringType(), True),
            ]
        )

        # EXPECTED OUTPUT 1: testing 2 days tolerance
        # Get expected output (with tolerance = 2*24*60*60 seconds which is the same as 2 days)
        expected_output_first_test = spark.createDataFrame(
            [
                (1, datetime(2020, 1, 1, 3, 23, 33), "FirstValue"),
                (1, datetime(2020, 1, 2, 16, 45, 0), "FirstValue"),
                (
                    1,
                    datetime(2020, 1, 3, 23, 59, 59),
                    None,
                ),  # The difference is more than 2*24*60*60 seconds (2 days)
                (
                    1,
                    datetime(2020, 1, 4, 8, 23, 24),
                    None,
                ),  # The difference between 20200104 and 20200101 is now three days, which is above the tolerance level of 2 days
                (
                    2,
                    datetime(2020, 12, 9, 3, 53, 11),
                    "ThirdValue",
                ),  # There are two dates that are equally distant from the exact date. It will pick the older date.
                (2, datetime(2020, 12, 18, 9, 13, 54), None),  # Above tolerance level
                (2, datetime(2021, 1, 2, 15, 30, 0), None),  # Above tolerance level
                (3, datetime(2020, 12, 9, 3, 53, 11), None),  # Won't match on key
            ],
            input_schema_expected_output_df,
        )

        # Get output
        output_first_test = DataframeUtilities.join_on_closest_time_match(
            primary_df,
            secondary_df,
            "Time",
            "Time_secondary",
            ["Key"],
            2 * 24 * 60 * 60,
        )

        # EXPECTED OUTPUT 2: testing 10 days tolerance
        # Get expected output (with tolerance = 10*24*60*60 seconds which is 10 days)
        expected_output_second_test = spark.createDataFrame(
            [
                (1, datetime(2020, 1, 1, 3, 23, 33), "FirstValue"),
                (1, datetime(2020, 1, 2, 16, 45, 0), "FirstValue"),
                (1, datetime(2020, 1, 3, 23, 59, 59), "FirstValue"),
                (
                    1,
                    datetime(2020, 1, 4, 8, 23, 24),
                    "FirstValue",
                ),  # The difference between 20200104 and 20200101 is now three days, which is below the tolerance level of 2 days
                (
                    2,
                    datetime(2020, 12, 9, 3, 53, 11),
                    "ThirdValue",
                ),  # There are two dates that are equally distant from the exact date. It will pick the older date.
                (
                    2,
                    datetime(2020, 12, 18, 9, 13, 54),
                    "SecondValue",
                ),  # Below tolerance level (8 days)
                (2, datetime(2021, 1, 2, 15, 30, 0), None),  # Above tolerance level
                (3, datetime(2020, 12, 9, 3, 53, 11), None),  # Won't match on key
            ],
            input_schema_expected_output_df,
        )

        # Get output
        output_second_test = join_on_closest_time_match(
            primary_df,
            secondary_df,
            "Time",
            "Time_secondary",
            ["Key"],
            10 * 24 * 60 * 60,
        )

        # EXPECTED OUTPUT 3: testing 103000 seconds tolerance which is approximatedly 28.6 hours
        # Get expected output (with tolerance = 103000 seconds which is ca. 28.6 hours)
        expected_output_third_test = spark.createDataFrame(
            [
                (1, datetime(2020, 1, 1, 3, 23, 33), "FirstValue"),
                (1, datetime(2020, 1, 2, 16, 45, 0), None),  # Above tolerance level
                (1, datetime(2020, 1, 3, 23, 59, 59), None),  # Above tolerance level
                (1, datetime(2020, 1, 4, 8, 23, 24), None,),  # Above tolerance level
                (
                    2,
                    datetime(2020, 12, 9, 3, 53, 11),
                    "ThirdValue",
                ),  # There are two dates that are equally distant from the exact date. It will pick the older date.
                (
                    2,
                    datetime(2020, 12, 18, 9, 13, 54),
                    None,
                ),  # Below tolerance level (8 days)
                (2, datetime(2021, 1, 2, 15, 30, 0), None),  # Above tolerance level
                (3, datetime(2020, 12, 9, 3, 53, 11), None),  # Won't match on key
            ],
            input_schema_expected_output_df,
        )

        # Get output
        output_third_test = DataframeUtilities.join_on_closest_time_match(
            primary_df, secondary_df, "Time", "Time_secondary", ["Key"], 103000
        )

        # Get test result for all different test cases
        expected_output_list = [
            expected_output_first_test,
            expected_output_second_test,
            expected_output_third_test,
        ]
        output_list = [output_first_test, output_second_test, output_third_test]
