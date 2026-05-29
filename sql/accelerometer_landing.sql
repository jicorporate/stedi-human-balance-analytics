CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
    user string,
    timeStamp bigint,
    x double,
    y double,
    z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-human-balance-analytics-jicorporate/accelerometer/landing/';