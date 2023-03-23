# dataLake-template

## 1. Introduction

Some demos of using Spark to write MySQL and Kafka data to data lake,such as Delta,Hudi,Iceberg.

## 2. Version info

`SPARK` 3.1.2

`DELTA` 1.0.0

`HUDI` 0.10.1

`ICEBERG` 0.12.1

`MYSQL` 8.0.24

## 3 Kafka to Data Lake

### 3.1 read Kafka

use [kafka.py](./scipt/kafka.py) to generate data.
`kafka.bootstrap.servers` and `subscribe` is necessary

```scala
import sparkSession.implicits._
val df: DataFrame = sparkSession
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaConsumer)
  .option("subscribe", kafkaTopic)
  .option("startingOffsets", startingOffsets)
  .option("failOnDataLoss", failOnDataLoss)
  .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
  .load()

val frame: Dataset[Row] = df.select(from_json('value.cast("string"), schema) as "value").select($"value.*")
```

### 3.2 write to Delta

```scala
val query: StreamingQuery = frame
  .writeStream
  .format("delta")
  .option("path", deltaPath)
  .option("checkpointLocation", deltaPath)
  .partitionBy(partitionColumns:_*)
  .start()
```

### 3.3 write to Hudi

```scala
val query: StreamingQuery = frame
  .writeStream
  .format("hudi")
  .option(RECORDKEY_FIELD.key, "user_id")
  .option(PRECOMBINE_FIELD.key, "user_id")
  .option(PARTITIONPATH_FIELD.key(), partitionFields)
  .option(HoodieWriteConfig.TBL_NAME.key, hoodieTableName)
  .outputMode("append")
  .option("path", lakePath)
  .option("checkpointLocation", checkpointLocation)
  .trigger(Trigger.ProcessingTime(10,TimeUnit.SECONDS))
  .start()
```

### 3.4 write to Iceberg

create iceberg table first

```scala
//The table should be created in prior to start the streaming query.
//Refer SQL create table on Spark page to see how to create the Iceberg table.
sparkSession.sql("create table czl_iceberg.demo.kafka_spark_iceberg (" +
 "user_id bigint, station_time string, score integer, local_time timestamp" +
  ") using iceberg")

val query: StreamingQuery = frame
  .writeStream
  .format("iceberg")
  //Iceberg supports append and complete output modes:
  //append: appends the rows of every micro-batch to the table
  //complete: replaces the table contents every micro-batch
  .outputMode("append")
  .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
  .option("path", icebergPath)
  .option("checkpointLocation", checkpointPath)
  .start()
```

## 4 Mysql to Data Lake

### 4.1 read Mysql

```scala
val readMysql: DataFrame = sparkSession
  .read
  .format("jdbc")
  .option("url", url)
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", tableName)
  .option("user", username)
  .option("password", password).load()
```

### 4.2 write to Delta

```scala
readMysql
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .partitionBy(partitionColumns:_*)
  .save(path)
```

### 4.3 write to Hudi

```scala
readMysql
  .write
  .format("hudi")
  .option(RECORDKEY_FIELD.key, "id")
  .option(PRECOMBINE_FIELD.key, "id")
  .option(HoodieWriteConfig.TBL_NAME.key, hoodieTableName)
  .mode(SaveMode.Overwrite)
  .partitionBy(partitionColumns:_*)
  .save(path)
```

### 4.4 write to Iceberg

```scala
readMysql
  .writeTo(path)
  .create()
```

