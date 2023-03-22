package com.czl.datalake.template.iceberg.mysql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/7
 * Description:
 */
object MysqlToIceberg {

  def main(args: Array[String]): Unit = {

    val url: String = "jdbc:mysql://ip:port"
    val tableName: String = "db.table"
    val username: String = "root"
    val password: String = "root"
    //catalog.db.table
    val path: String = "czl_iceberg.demo.mysql_spark_iceberg"
    val lakePath: String = "testData/mysql_spark_iceberg"
    val partitionColumns: Array[String] = null

    val sparkSession: SparkSession = SparkSession.builder().master("local[*]")
      //define catalog name
      .config("spark.sql.catalog.czl_iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.czl_iceberg.type", "hadoop")
      .config("spark.sql.catalog.czl_iceberg.warehouse", lakePath)
      .getOrCreate()

    val readMysql: DataFrame = sparkSession.read.format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", username)
      .option("password", password).load()

    if (null == partitionColumns) {
      readMysql
        .writeTo(path)
        .create()
    } else {
      val array: Array[Column] = partitionColumns.map((field: String) => column(field))
      readMysql
        .sortWithinPartitions(partitionColumns.head, partitionColumns.slice(1,partitionColumns.length):_*)
        .writeTo(path)
        .partitionedBy(array.head, array.slice(1,array.length):_*)
        .create()
    }
  }
}
