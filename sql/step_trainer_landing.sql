CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
    sensorReadingTime bigint,
    serialNumber string,
    distanceFromObject int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-human-balance-analytics-jicorporate/step_trainer/landing/';