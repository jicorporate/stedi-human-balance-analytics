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

STEP_TRAINER_TRUSTED_PATH = (
    "s3://stedi-human-balance-analytics-jicorporate/step_trainer/trusted/"
)


step_trainer_landing_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_dynamic_frame"
)

customer_curated_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name="customer_curated",
    transformation_ctx="customer_curated_dynamic_frame"
)


step_trainer_landing_df = step_trainer_landing_dynamic_frame.toDF()
customer_curated_df = customer_curated_dynamic_frame.toDF()


step_trainer_landing_count = step_trainer_landing_df.count()
customer_curated_count = customer_curated_df.count()

print("step_trainer_landing_count =", step_trainer_landing_count)
print("customer_curated_count =", customer_curated_count)

step_trainer_landing_df.printSchema()
customer_curated_df.printSchema()


if step_trainer_landing_count != 28680:
    raise Exception(
        "Invalid step_trainer_landing_count. Expected 28680 but got "
        + str(step_trainer_landing_count)
    )

if customer_curated_count != 482:
    raise Exception(
        "Invalid customer_curated_count. Expected 482 but got "
        + str(customer_curated_count)
    )


step_trainer_landing_df.createOrReplaceTempView("step_trainer_landing")
customer_curated_df.createOrReplaceTempView("customer_curated")


step_trainer_trusted_df = spark.sql("""
    SELECT DISTINCT
        s.sensorreadingtime,
        s.serialnumber,
        s.distancefromobject
    FROM step_trainer_landing s
    INNER JOIN customer_curated c
        ON s.serialnumber = c.serialnumber
""")


step_trainer_trusted_count = step_trainer_trusted_df.count()

print("step_trainer_trusted_count =", step_trainer_trusted_count)


if step_trainer_trusted_count != 14460:
    raise Exception(
        "Invalid step_trainer_trusted_count. Expected 14460 but got "
        + str(step_trainer_trusted_count)
    )


step_trainer_trusted_dynamic_frame = DynamicFrame.fromDF(
    step_trainer_trusted_df,
    glueContext,
    "step_trainer_trusted_dynamic_frame"
)


step_trainer_trusted_sink = glueContext.getSink(
    path=STEP_TRAINER_TRUSTED_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_sink"
)

step_trainer_trusted_sink.setCatalogInfo(
    catalogDatabase=DATABASE_NAME,
    catalogTableName="step_trainer_trusted"
)

step_trainer_trusted_sink.setFormat("json")
step_trainer_trusted_sink.writeFrame(step_trainer_trusted_dynamic_frame)


job.commit()