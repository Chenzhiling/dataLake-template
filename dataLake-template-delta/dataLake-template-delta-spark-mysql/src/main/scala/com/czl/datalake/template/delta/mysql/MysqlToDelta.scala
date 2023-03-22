package com.czl.datalake.template.delta.mysql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/22
 * Description:
 */
object MysqlToDelta {

  def main(args: Array[String]): Unit = {

    val url: String = "jdbc:mysql://ip:port"
    val tableName: String = "db.table"
    val username: String = "root"
    val password: String = "root"
    val partitionColumns: Array[String] = Array("bit_test")
    val path: String = "./testData/mysql_spark_delta"

    val sparkSession: SparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.schema.autoMerge.enabled",value = true)
      .getOrCreate()

    val readMysql: DataFrame = sparkSession.read.format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", username)
      .option("password", password).load()

    readMysql
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionColumns:_*)
      .save(path)
  }
}
