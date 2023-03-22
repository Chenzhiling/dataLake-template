package com.czl.datalake.template.hudi.kafka

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.config.HoodieWriteConfig
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
object KafkaToHudi {

  def main(args: Array[String]): Unit = {

    val kafkaConsumer: String = "kafka consumer"
    val kafkaTopic: String = "topic"
    val startingOffsets: String = "latest"
    val failOnDataLoss: String = "true"
    val maxOffsetsPerTrigger: Int = 3000
    val hoodieTableName : String  = "hoodieTableName"
    val lakePath : String = "lakePath"
    val checkpointLocation : String = "checkpointLocation"
    val partitionFields : String = Array().mkString(",")

    val schema: StructType = StructType(List(
      StructField("user_id", LongType),
      StructField("station_time", StringType),
      StructField("score", IntegerType),
      StructField("local_time", TimestampType)
    ))

    val sparkSession: SparkSession = SparkSession.builder().master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .getOrCreate()

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

    query.awaitTermination()
  }
}
