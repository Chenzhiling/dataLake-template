package com.czl.datalake.template.delta.kafka

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/22
 * Description:
 */
object KafkaToDelta {

  def main(args: Array[String]): Unit = {

    val kafkaConsumer: String = "ip"
    val kafkaTopic: String = "topic"
    // latest or earliest
    val startingOffsets: String = "latest"
    //true or false
    val failOnDataLoss: String = "true"
    val maxOffsetsPerTrigger: Int = 3000
    val deltaPath : String = "path"
    val schema: StructType = StructType(List(
      StructField("user_id", LongType),
      StructField("station_time", StringType),
      StructField("score", IntegerType),
      StructField("local_time", TimestampType)
    ))
    val partitionColumns: Array[String] = null

    val sparkSession: SparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
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

    val query: StreamingQuery = frame.writeStream.format("delta")
      .option("path", deltaPath)
      .option("checkpointLocation", deltaPath)
      .partitionBy(partitionColumns:_*)
      .start()

    query.awaitTermination()
  }
}
