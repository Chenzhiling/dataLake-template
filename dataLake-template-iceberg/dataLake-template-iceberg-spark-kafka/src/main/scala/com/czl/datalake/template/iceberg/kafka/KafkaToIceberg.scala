package com.czl.datalake.template.iceberg.kafka

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util.concurrent.TimeUnit

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/22
 * Description:
 */
object KafkaToIceberg {

  def main(args: Array[String]): Unit = {

    val kafkaConsumer: String = "kafka ip"
    val kafkaTopic: String = "topic"
    val startingOffsets: String = "latest"
    val failOnDataLoss: String = "true"
    val maxOffsetsPerTrigger: Int = 3000
    val wareHousePath : String = "warehouse path"
    //A table name if the table is tracked by a catalog, like catalog.database.table_name
    val icebergPath : String = "czl_iceberg.demo.kafka_spark_iceberg"
    val checkpointPath : String = "checkpoint path"

    val schema: StructType = StructType(List(
      StructField("user_id", LongType),
      StructField("station_time", StringType),
      StructField("score", IntegerType),
      StructField("local_time", TimestampType)
    ))

    val sparkSession: SparkSession = SparkSession.builder().master("local[*]")
      //define catalog name
      .config("spark.sql.catalog.czl_iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.czl_iceberg.type", "hadoop")
      .config("spark.sql.catalog.czl_iceberg.warehouse", wareHousePath)
      .getOrCreate()

    //The table should be created in prior to start the streaming query.
    //Refer SQL create table on Spark page to see how to create the Iceberg table.
    sparkSession.sql("create table czl_iceberg.demo.kafka_spark_iceberg (" +
      "user_id bigint, station_time string, score integer, local_time timestamp" +
      ") using iceberg")

    import sparkSession.implicits._
    val df: DataFrame = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConsumer)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", failOnDataLoss)
      .option("maxOffsetsPerTrigger",maxOffsetsPerTrigger)
      .load()

    val frame: Dataset[Row] = df
      .select(from_json('value.cast("string"), schema) as "value")
      .select($"value.*")

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
    query.awaitTermination()
  }
}
