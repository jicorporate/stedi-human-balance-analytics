CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
    serialNumber string,
    shareWithPublicAsOfDate bigint,
    birthDay string,
    registrationDate bigint,
    shareWithResearchAsOfDate bigint,
    customerName string,
    email string,
    lastUpdateDate bigint,
    phone string,
    shareWithFriendsAsOfDate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-human-balance-analytics-jicorporate/customer/landing/';