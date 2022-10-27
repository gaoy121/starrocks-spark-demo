import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object spark_Connector_dm2sr {
  def main(args: Array[String]): Unit = {
    val JDBC_URL = "jdbc:dm://192.168.110.19:5236/dmdb_test"
    val conf = new SparkConf().setMaster("local[*]").setAppName("dm")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    /* val jdbcDF = spark.read
       .format("jdbc")
       .option("url", "jdbc:dm://192.168.110.19:5236/dmdb_test")
       .option("user", "")
       .option("password", "")
       .option("", "")
       .load()*/
    val prop = new Properties()
    prop.setProperty("user", "SYSDBA")
    prop.setProperty("password", "123456789")
    val jdbcDF = spark.read.
      jdbc("jdbc:dm://192.168.110.19:5236/dmdb_test", " student", prop)
    jdbcDF.show()
    jdbcDF.write
      .format("starrocks")
      .option("starrocks.table.identifier", "gyuan.sqlserver_spark_connrctor_starrocks")
      .option("starrocks.fenodes", "192.168.110.101:8030")
      .option("starrocks.benodes", "192.168.110.101:8040")
      .option("user", "root")
      .option("password", "root")
      .save()
    spark.stop()
  }
}
