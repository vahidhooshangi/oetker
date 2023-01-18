import logging
from pyspark.sql import functions as func
from pyspark.sql import (SparkSession,
                         Window)

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("PySpark Read JSON Oetker") \
    .getOrCreate()


def convert_date(spark_df):
    """
    convert the string date to the date data type.

    :param spark_df: the pyspark dataframe
    :return: the dataframe with date data type
    """
    return spark_df.withColumn('date', func.to_date('date', 'dd/MM/yyyy'))


def find_min_max_country_frequencies(spark_df) -> None:
    """
    The parent procedure for finding the min/max of the country frequencies in data set.
    prepare data frame by adding count of frequency of each country.

    :param spark_df: the pyspark dataframe
    """
    count_per_country = func.count('country').over(Window.partitionBy('country')).alias('count_per_country')
    spark_df = spark_df.select('*', count_per_country)

    logging.info(find_min_country_frequencies(spark_df))
    logging.info(find_max_country_frequencies(spark_df))


def find_min_country_frequencies(spark_df) -> str:
    """
    Find the name of the country has minimum frequency

    :param spark_df: the pyspark dataframe
    :return: The name of the country and it's frequencies in data frame
    """
    df_min_country_count = spark_df.groupBy().agg(func.min('count_per_country').alias('count_per_country'))
    df_min_country_name = spark_df.join(df_min_country_count, ['count_per_country'], 'inner')
    collect = df_min_country_name.select('country', 'count_per_country').distinct().collect()

    key = collect[0]['count_per_country']
    temp_dictionary = {key: [x['country'] for x in collect]}

    return 'Minimum freq is : {} for {} '.format(key, temp_dictionary[key])


def find_max_country_frequencies(spark_df) -> str:
    """
    Find the name of the country has maximum frequency

    :param spark_df: the pyspark dataframe
    :return: The name of the country and it's frequencies in data frame
    """
    df_max_country_count = spark_df.groupBy().agg(func.max('count_per_country').alias('count_per_country'))
    df_max_country_name = spark_df.join(df_max_country_count, ['count_per_country'], 'inner')
    collect = df_max_country_name.select('country', 'count_per_country').distinct().collect()

    key = collect[0]['count_per_country']
    temp_dictionary = {key: [x['country'] for x in collect]}

    return 'Minimum freq is : {} for {} '.format(key, temp_dictionary[key])


def find_number_unique_user(spark_df) -> str:
    """
    find the unique number of the user based on the [first_name, last_name, email]

    :param spark_df: the pyspark dataframe
    :return: the unique number of the user
    """
    distinct_df = spark_df.select(func.countDistinct('first_name', 'last_name', 'email').alias('distinct_count'))

    return "number of the unique user is : {}".format(distinct_df.collect()[0]['distinct_count'])


def filter_invalid_date(spark_df):
    """
    filter all rows has invalid date

    :param spark_df: the pyspark dataframe
    :return: the dataframe without invalid date
    """
    return spark_df.filter(spark_df['date'].isNotNull())


def filter_invalid_ip(spark_df):
    """
    flag and filter all invalid records contain invalid IP

    :param spark_df: the pyspark dataframe
    :return: the dataframe without invalid IP
    """
    spark_df = spark_df.withColumn('ip_split', func.split('ip_address', '\.')). \
        withColumn('valid_ip_flag',
                   func.size(func.expr(
                       'filter(ip_split, x -> x between 0 and 255)')) == 4
                   )

    # clean exclude the invalid ip rows
    return spark_df.filter(True == spark_df['valid_ip_flag'])


def init_cap_country(spark_df):
    """
    convert all countries to the first letter capital
    germany => Germany

    :param spark_df: the pyspark dataframe
    :return: a dataframe contains a country column with first letter capital role
    """
    return spark_df.withColumn('country', func.initcap(spark_df['country']))


def clean_dataframe(spark_df):
    """
    Apply all roles for cleaning input data set.

    :param spark_df: the pyspark dataframe
    :return: the clean dataframe
    """
    spark_df = filter_invalid_ip(spark_df)
    spark_df = convert_date(spark_df)
    spark_df = filter_invalid_date(spark_df)
    spark_df = init_cap_country(spark_df)

    return spark_df


def make_data_frame(spark_session):
    """
    make the dataframe from the file

    :param spark_session: The spark session object
    :return: the raw pyspark dataframe
    """
    return spark_session.read.json("{{ params.file_path }}",
                                   multiLine=True)


df = make_data_frame(spark)
clean_dataframe(df)

logging.info(find_number_unique_user(df))
