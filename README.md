```sql
create external table `syslogrecord` (
`dateTime` string , `client` string , `messageID` string , `messageString` string
)
stored as parquet
location '/user/hive/syslog/v1/syslogrecord'
```

```
tail -f /var/log/syslog |nc -l 7088
```

```
spark-submit --master local[2] --class org.apache.spark.examples.streaming.SysLogReceiver /tmp/milan/spark-streaming-kafka_2.10-1.0.jar 172.28.128.11 7088
```