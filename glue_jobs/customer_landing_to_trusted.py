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

CUSTOMER_LANDING_PATH = (
    "s3://stedi-human-balance-analytics-jicorporate/customer/landing/"
)

CUSTOMER_TRUSTED_PATH = (
    "s3://stedi-human-balance-analytics-jicorporate/customer/trusted/"
)


customer_landing_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [CUSTOMER_LANDING_PATH],
        "recurse": True
    },
    format="json",
    transformation_ctx="customer_landing_dynamic_frame"
)


customer_landing_df = customer_landing_dynamic_frame.toDF()

customer_landing_count = customer_landing_df.count()
print("customer_landing_count =", customer_landing_count)

customer_landing_df.printSchema()

customer_landing_df.createOrReplaceTempView("customer_landing")


customer_trusted_df = spark.sql("""
    SELECT
        serialnumber,
        sharewithpublicasofdate,
        birthday,
        registrationdate,
        sharewithresearchasofdate,
        customername,
        email,
        lastupdatedate,
        phone,
        sharewithfriendsasofdate
    FROM customer_landing
    WHERE sharewithresearchasofdate IS NOT NULL
""")


customer_trusted_count = customer_trusted_df.count()
print("customer_trusted_count =", customer_trusted_count)

if customer_landing_count != 956:
    raise Exception(
        "Invalid customer_landing_count. Expected 956 but got "
        + str(customer_landing_count)
    )

if customer_trusted_count != 482:
    raise Exception(
        "Invalid customer_trusted_count. Expected 482 but got "
        + str(customer_trusted_count)
    )


customer_trusted_dynamic_frame = DynamicFrame.fromDF(
    customer_trusted_df,
    glueContext,
    "customer_trusted_dynamic_frame"
)


customer_trusted_sink = glueContext.getSink(
    path=CUSTOMER_TRUSTED_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_sink"
)

customer_trusted_sink.setCatalogInfo(
    catalogDatabase=DATABASE_NAME,
    catalogTableName="customer_trusted"
)

customer_trusted_sink.setFormat("json")
customer_trusted_sink.writeFrame(customer_trusted_dynamic_frame)


job.commit()