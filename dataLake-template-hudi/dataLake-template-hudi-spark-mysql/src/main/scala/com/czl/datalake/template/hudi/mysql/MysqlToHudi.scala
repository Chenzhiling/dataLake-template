package com.czl.datalake.template.hudi.mysql

import org.apache.hudi.DataSourceWriteOptions.{PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/22
 * Description:
 */
object MysqlToHudi {


  val url: String = "jdbc:mysql://ip:port"
  val tableName: String = "db.table"
  val username: String = "root"
  val password: String = "root"
  val partitionColumns: Array[String] = Array()
  val hoodieTableName: String = "mysql_spark_hudi"
  val path: String = "absolute path"


  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .getOrCreate()

    val readMysql: DataFrame = sparkSession.read.format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", username)
      .option("password", password).load()

    readMysql.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(HoodieWriteConfig.TBL_NAME.key, hoodieTableName)
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionColumns:_*)
      .save(path)
  }
}
