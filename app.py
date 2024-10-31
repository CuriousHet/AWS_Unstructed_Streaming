from pyspark.sql import SparkSession

from config.config import configuration
from pyspark.sql.types import  StructType,StructField,StringType, DoubleType,DateType
from udf_utils import *
from pyspark.sql.functions import udf,regexp_replace,DataFrame


def define_udfs():
    return {

        'extract_file_name_udf': udf(extract_file_name, StringType()),
        'extract_position_udf': udf(extract_position, StringType()),
        'extract_salary_udf': udf(extract_salary, StructType([
            StructField('salary_start',DoubleType(),True),
            StructField('salary_end',DoubleType(),True)
        ])),
        'extract_startdate_udf': udf(extract_start_date, DateType()),
        'extract_classcode_udf': udf(extract_class_code, StringType()),
        'extract_requirements_udf': udf(extract_requirements, StringType()),
        'extract_notes_udf': udf(extract_notes, StringType()),
        'extract_duties_udf': udf(extract_duties, StringType()),
        'extract_selection_udf': udf(extract_selection, StringType()),
        'extract_experience_length_udf': udf(extract_experience_length, StringType()),
        'extract_education_udf': udf(extract_education, StringType()),
        'extract_application_location_udf': udf(extract_application_location,StringType()),
        'extract_job_type_udf':udf(extract_job_type,StringType())
    }

if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .appName("AWS_Spark_Unstructure")
        .config(
            'spark.jars.packages',
            'org.apache.hadoop:hadoop-aws:3.3.1,'
            'com.amazonaws:aws-java-sdk:1.11.469'
        )
        .config("spark.executor.memory", "4g")
        .config('spark.hadoop.fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.access.key',configuration.get('AWS_ACCESS_KEY'))
        .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY'))
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .getOrCreate()
    )

    text_input_dir = 'file:///F:/AWS_spark_unstructure/input/input_text'
    json_input_dir = 'file:///F:/AWS_spark_unstructure/input/input_json'
    csv_input_dir = 'file:///F:/AWS_spark_unstructure/input/input_csv'
    video_input_dir = 'file:///F:/AWS_spark_unstructure/input/input_video'
    pdf_input_dir = 'file:///F:/AWS_spark_unstructure/input/input_pdf'
    img_input_dir = 'file:///F:/AWS_spark_unstructureinput/input_img'


    data_schema = StructType([

        StructField('file_name',StringType(), True),
        StructField('position',StringType(), True),
        StructField('classcode',StringType(), True),
        StructField('salary_start',DoubleType(), True),
        StructField('salary_end',DoubleType(), True),
        StructField('start_date',DateType(), True),
        StructField('req',StringType(), True),
        StructField('notes',StringType(), True),
        StructField('duties',StringType(), True),
        StructField('selection',StringType(), True),
        StructField('experience_length',StringType(), True),
        StructField('job_type',StringType(), True),
        StructField('education_length',StringType(), True),
        StructField('application_location',StringType(), True),

    ])

    udfs = define_udfs()

    job_bulletins_df = (spark.readStream
                        .format('text')
                        .option('wholetext','true')
                        .load(text_input_dir)
                        )

    json_df = spark.readStream.json(json_input_dir,schema=data_schema,multiLine=True)

    job_bulletins_df = job_bulletins_df.withColumn('file_name',
                                                   regexp_replace(udfs['extract_file_name_udf']('value'),r'\r',' '))
    job_bulletins_df = job_bulletins_df.withColumn('classcode', udfs['extract_classcode_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('position',regexp_replace(udfs['extract_position_udf']('value'),r'\r',' '))
    job_bulletins_df = job_bulletins_df.withColumn('salary_start',udfs['extract_salary_udf']('value').getField('salary_start'))
    job_bulletins_df = job_bulletins_df.withColumn('salary_end',udfs['extract_salary_udf']('value').getField('salary_end'))

    job_bulletins_df = job_bulletins_df.withColumn('start_date',udfs['extract_startdate_udf']('value'))

    job_bulletins_df = job_bulletins_df.withColumn('req',
                                                   regexp_replace(udfs['extract_requirements_udf']('value'), r'\r', ' '))

    job_bulletins_df = job_bulletins_df.withColumn('notes',
                                               regexp_replace(udfs['extract_notes_udf']('value'), r'\r', ' '))

    job_bulletins_df = job_bulletins_df.withColumn('duties',
                                               regexp_replace(udfs['extract_duties_udf']('value'), r'\r', ' '))

    job_bulletins_df = job_bulletins_df.withColumn('selection',
                                               regexp_replace(udfs['extract_selection_udf']('value'), r'\r', ' '))

    job_bulletins_df = job_bulletins_df.withColumn('experience_length',
                                                   regexp_replace(udfs['extract_experience_length_udf']('value'), r'\r', ' '))

    job_bulletins_df = job_bulletins_df.withColumn('education_length',
                                               regexp_replace(udfs['extract_education_udf']('value'), r'\r', ' '))

    job_bulletins_df = job_bulletins_df.withColumn('application_location',
                                               regexp_replace(udfs['extract_application_location_udf']('value'), r'\r', ' '))
    job_bulletins_df = job_bulletins_df.withColumn('job_type',regexp_replace(udfs['extract_job_type_udf']('value'), r'\r', ' '))

    # # Selecting relevant columns for output DataFrame# Select columns from job_bulletins_df

    j_df = job_bulletins_df.select('file_name', 'salary_start', 'salary_end', 'position',
                                   'start_date', 'req', 'notes', 'job_type', 'classcode',
                                   'duties', 'selection', 'experience_length',
                                   'education_length', 'application_location')

    # Select the same columns from json_df
    json_df = json_df.select('file_name', 'salary_start', 'salary_end', 'position',
                             'start_date', 'req', 'notes', 'job_type', 'classcode',
                             'duties', 'selection', 'experience_length',
                             'education_length', 'application_location')

    # Union the two DataFrames
    union_df = j_df.union(json_df)

    def streamWriter(input: DataFrame,checkpointFolder,output):

        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation',checkpointFolder)
                .option('path',output)
                .outputMode('append')
                .trigger(processingTime='5 seconds')
                .start()
                )

    # query = (union_df
    #          .writeStream
    #          .outputMode('append')
    #          .format('console')
    #          .option('truncate', False)
    #          .start()
    #          )

    query = streamWriter(union_df,'s3a://spark-unstructed-streaming/checkpoints/','s3a://spark-unstructed-streaming/data/spark_unstructed')

    query.awaitTermination()

    spark.stop()
