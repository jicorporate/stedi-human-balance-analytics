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

CUSTOMER_TRUSTED_PATH = (
    "s3://stedi-human-balance-analytics-jicorporate/customer/trusted/"
)


customer_landing_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name="customer_landing",
    transformation_ctx="customer_landing_dynamic_frame"
)


customer_landing_df = customer_landing_dynamic_frame.toDF()

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