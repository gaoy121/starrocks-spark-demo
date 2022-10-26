import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object spark_Connector_sqlserver2sr {
  def main(args: Array[String]): Unit = {
    val dbServer = "192.168.110.19"
    val db = "source_sqlserver"
    var user="sa"
    val password="123456789"

    val url = "jdbc:sqlserver://192.168.110.19:1433;databaseName=source_sqlserver"

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", url)
      .option("user", "sa")
      .option("password", "123456789")
      .option("dbtable", "dbo.student")
      .load()
    jdbcDF.show
      jdbcDF.write
        .format("starrocks")
        .option("starrocks.table.identifier", "gyuan.sqlserver_spark_connrctor_starrocks")
        .option("starrocks.fenodes", "192.168.110.101:8030")
        .option("starrocks.benodes","192.168.110.101:8040")
        .option("user", "root")
        .option("password", "root")
        .save()
    spark.stop()
  }
}
