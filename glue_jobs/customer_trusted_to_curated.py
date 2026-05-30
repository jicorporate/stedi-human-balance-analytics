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

CUSTOMER_CURATED_PATH = (
    "s3://stedi-human-balance-analytics-jicorporate/customer/curated/"
)


customer_trusted_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_dynamic_frame"
)

accelerometer_trusted_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_dynamic_frame"
)


customer_trusted_df = customer_trusted_dynamic_frame.toDF()
accelerometer_trusted_df = accelerometer_trusted_dynamic_frame.toDF()


customer_trusted_count = customer_trusted_df.count()
accelerometer_trusted_count = accelerometer_trusted_df.count()

print("customer_trusted_count =", customer_trusted_count)
print("accelerometer_trusted_count =", accelerometer_trusted_count)

customer_trusted_df.printSchema()
accelerometer_trusted_df.printSchema()


if customer_trusted_count != 482:
    raise Exception(
        "Invalid customer_trusted_count. Expected 482 but got "
        + str(customer_trusted_count)
    )

if accelerometer_trusted_count != 40981:
    raise Exception(
        "Invalid accelerometer_trusted_count. Expected 40981 but got "
        + str(accelerometer_trusted_count)
    )


customer_trusted_df.createOrReplaceTempView("customer_trusted")
accelerometer_trusted_df.createOrReplaceTempView("accelerometer_trusted")


customer_curated_df = spark.sql("""
    SELECT DISTINCT
        c.serialnumber,
        c.sharewithpublicasofdate,
        c.birthday,
        c.registrationdate,
        c.sharewithresearchasofdate,
        c.customername,
        c.email,
        c.lastupdatedate,
        c.phone,
        c.sharewithfriendsasofdate
    FROM customer_trusted c
    INNER JOIN accelerometer_trusted a
        ON c.email = a.user
""")


customer_curated_count = customer_curated_df.count()

print("customer_curated_count =", customer_curated_count)


if customer_curated_count != 482:
    raise Exception(
        "Invalid customer_curated_count. Expected 482 but got "
        + str(customer_curated_count)
    )


customer_curated_dynamic_frame = DynamicFrame.fromDF(
    customer_curated_df,
    glueContext,
    "customer_curated_dynamic_frame"
)


customer_curated_sink = glueContext.getSink(
    path=CUSTOMER_CURATED_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_sink"
)

customer_curated_sink.setCatalogInfo(
    catalogDatabase=DATABASE_NAME,
    catalogTableName="customer_curated"
)

customer_curated_sink.setFormat("json")
customer_curated_sink.writeFrame(customer_curated_dynamic_frame)


job.commit()