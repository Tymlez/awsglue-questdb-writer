# AWS Glue / PySpark QuestDB writer
A simple writing utility for writing to QuestDB from PySpark / AWS Glue

#Introduction
A very simple convenience library created due to difficulty in getting custom builds of the AWS Glue Libs for AWS for local development.
The standard release is Python 3.6 and requires some changes to be made in order to add in extra libraries.

The InfluxDB writer is a potential alternative to this, but I didn't have much of a chance to get it working due
to dependencies and it not easily supporting PySpark.

#Installation

Install this via pip

`pip3 install awsglue-questdb-writer`


#Usage

In your AWS Glue / PySpark Job include the file via

`from awsglue_questdb_writer import *`

Usage is by passing a DF to the function, this should ideally be a DF from a SparkSQL output like Glue creates (e.g. from the from_catalog) as that is what has been tested.

**Important to note:**

- All Timestamps must be datetime objects
- Nanosecond precision (required by QuestDB) is currently only your timestamp precision with added zeros
- If you need real nanosecond permission you must be on Python 3.7 and update the library to use it (See comments)
- QuestDB is whitespace sensitive, all datetimes are quoted but any other fields with whitespace will cause this to fail (silently)
- There is no socket response from this library (it is designed to be unmonitored and high throughput) - if errors are in your input it will fail silently (PR's welcome)
- There is a convenience line to drop unwanted fields prior to passing this into the function to write to QuestDB

```python

args = getResolvedOptions(sys.argv,
                          ['TempDir', 'JOB_NAME', 'db_name', 'temp_workflow_bucket', 'questdb_host', 'questdb_port'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

allDaily = glueContext.create_dynamic_frame.from_catalog(database=args['db_name'],
                                                         table_name="daily",
                                                         transformation_ctx="allDaily",
                                                         )

df = allDaily.toDF()
tdf = df.withColumn('reading_date_time', F.to_timestamp(df['reading_date_time'], '%Y-%m-%dT%H:%M:%S.%f'))
tdf = tdf.drop(*["ingestion_date", "period_start", "period_end", "quality_method",
                 "event", "import_reactive_total", "export_reactive_total"])

write_to_quest(df=tdf, measurement="meter_id", table="daily", timestamp_field="reading_date_time", args=args)

job.commit()

```

#License
See LICENSE for full details
