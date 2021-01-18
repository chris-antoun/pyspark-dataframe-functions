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