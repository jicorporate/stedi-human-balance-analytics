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

MACHINE_LEARNING_CURATED_PATH = (
    "s3://stedi-human-balance-analytics-jicorporate/machine_learning/curated/"
)


step_trainer_trusted_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_dynamic_frame"
)

accelerometer_trusted_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_dynamic_frame"
)


step_trainer_trusted_df = step_trainer_trusted_dynamic_frame.toDF()
accelerometer_trusted_df = accelerometer_trusted_dynamic_frame.toDF()


step_trainer_trusted_count = step_trainer_trusted_df.count()
accelerometer_trusted_count = accelerometer_trusted_df.count()

print("step_trainer_trusted_count =", step_trainer_trusted_count)
print("accelerometer_trusted_count =", accelerometer_trusted_count)

step_trainer_trusted_df.printSchema()
accelerometer_trusted_df.printSchema()


if step_trainer_trusted_count != 14460:
    raise Exception(
        "Invalid step_trainer_trusted_count. Expected 14460 but got "
        + str(step_trainer_trusted_count)
    )

if accelerometer_trusted_count != 40981:
    raise Exception(
        "Invalid accelerometer_trusted_count. Expected 40981 but got "
        + str(accelerometer_trusted_count)
    )


step_trainer_trusted_df.createOrReplaceTempView("step_trainer_trusted")
accelerometer_trusted_df.createOrReplaceTempView("accelerometer_trusted")


machine_learning_curated_df = spark.sql("""
    SELECT
        s.sensorreadingtime,
        s.serialnumber,
        s.distancefromobject,
        a.user,
        a.`timestamp`,
        a.x,
        a.y,
        a.z
    FROM step_trainer_trusted s
    INNER JOIN accelerometer_trusted a
        ON s.sensorreadingtime = a.`timestamp`
""")


machine_learning_curated_count = machine_learning_curated_df.count()

print("machine_learning_curated_count =", machine_learning_curated_count)


if machine_learning_curated_count != 43681:
    raise Exception(
        "Invalid machine_learning_curated_count. Expected 43681 but got "
        + str(machine_learning_curated_count)
    )


machine_learning_curated_dynamic_frame = DynamicFrame.fromDF(
    machine_learning_curated_df,
    glueContext,
    "machine_learning_curated_dynamic_frame"
)


machine_learning_curated_sink = glueContext.getSink(
    path=MACHINE_LEARNING_CURATED_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_sink"
)

machine_learning_curated_sink.setCatalogInfo(
    catalogDatabase=DATABASE_NAME,
    catalogTableName="machine_learning_curated"
)

machine_learning_curated_sink.setFormat("json")
machine_learning_curated_sink.writeFrame(machine_learning_curated_dynamic_frame)


job.commit()