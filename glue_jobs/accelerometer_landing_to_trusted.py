import sys
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


DATABASE_NAME = "stedi"

ACCELEROMETER_TRUSTED_PATH = (
    "s3://stedi-human-balance-analytics-jicorporate/accelerometer/trusted/"
)


accelerometer_landing_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_dynamic_frame"
)

customer_trusted_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_dynamic_frame"
)


accelerometer_landing_df = accelerometer_landing_dynamic_frame.toDF()
customer_trusted_df = customer_trusted_dynamic_frame.toDF()


accelerometer_landing_count = accelerometer_landing_df.count()
customer_trusted_count = customer_trusted_df.count()

print("accelerometer_landing_count =", accelerometer_landing_count)
print("customer_trusted_count =", customer_trusted_count)

accelerometer_landing_df.printSchema()
customer_trusted_df.printSchema()


if accelerometer_landing_count != 81273:
    raise Exception(
        "Invalid accelerometer_landing_count. Expected 81273 but got "
        + str(accelerometer_landing_count)
    )

if customer_trusted_count != 482:
    raise Exception(
        "Invalid customer_trusted_count. Expected 482 but got "
        + str(customer_trusted_count)
    )


accelerometer_landing_df.createOrReplaceTempView("accelerometer_landing")
customer_trusted_df.createOrReplaceTempView("customer_trusted")


accelerometer_trusted_df = spark.sql("""
    SELECT
        a.user,
        a.`timestamp`,
        a.x,
        a.y,
        a.z
    FROM accelerometer_landing a
    INNER JOIN customer_trusted c
        ON a.user = c.email
""")


accelerometer_trusted_count = accelerometer_trusted_df.count()

print("accelerometer_trusted_count =", accelerometer_trusted_count)


if accelerometer_trusted_count != 40981:
    raise Exception(
        "Invalid accelerometer_trusted_count. Expected 40981 but got "
        + str(accelerometer_trusted_count)
    )


accelerometer_trusted_dynamic_frame = DynamicFrame.fromDF(
    accelerometer_trusted_df,
    glueContext,
    "accelerometer_trusted_dynamic_frame"
)


accelerometer_trusted_sink = glueContext.getSink(
    path=ACCELEROMETER_TRUSTED_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_sink"
)

accelerometer_trusted_sink.setCatalogInfo(
    catalogDatabase=DATABASE_NAME,
    catalogTableName="accelerometer_trusted"
)

accelerometer_trusted_sink.setFormat("json")
accelerometer_trusted_sink.writeFrame(accelerometer_trusted_dynamic_frame)


job.commit()
