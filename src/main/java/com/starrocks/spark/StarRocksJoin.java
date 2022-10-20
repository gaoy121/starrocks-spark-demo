package com.starrocks.spark;/**
 * @Author skyable
 * @Date 2022/9/15 13:20
 * @Desc
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.collection.Seq;

/**
 * @auther GaoYuan
 * @date 2022/9/15 13:20
 * @desc
 */
public class StarRocksJoin {

    // parameters
    private static String master = "local[2]";
    private static String appName = "app_spark_demo";

    private static String srcHost = "192.168.110.101";
    private static Integer srcPort = 8035;
    private static String srcUserName = "root";
    private static String srcPassword = "root";
    private static String srcDataBase = "gyuan";
    private static String srcTable = "users";

    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName(appName).setMaster(master));
        SQLContext sqlContext = new SQLContext(sparkContext);

        Dataset<Row> dataset1 = sqlContext.read().format("starrocks")
                .option("starrocks.table.identifier", srcDataBase + "." + srcTable)
                .option("starrocks.fenodes", srcHost + ":" + srcPort)
                .option("user", srcUserName)
                .option("password", srcPassword)
                .option("starrocks.filter.query","address is not null")
                .load();
//        dataset1.show();

        Dataset<Row> dataset2 = sqlContext.read().format("starrocks")
                .option("starrocks.table.identifier", srcDataBase + "." + srcTable)
                .option("starrocks.fenodes", srcHost + ":" + srcPort)
                .option("user", srcUserName)
                .option("password", srcPassword)
                .option("starrocks.filter.query","age > 20")
                .load();
//        dataset2.show();

        Dataset<Row> dataset = dataset1.as("d1").join(dataset2.as("d2"), dataset1.col("id").equalTo(dataset2.col("id")), "right")
                .where("d1.address is not null and d2.age > 20")
                .select("d1.id","d1.name","d1.address","d1.age");

//        Dataset<Row> dataset = dataset2.as("d2").join(dataset1.as("d1"), dataset2.col("id").equalTo(dataset1.col("id")), "left")
//                .select("d1.id","d1.name","d1.address","d1.age");

        dataset.show();
    }

}
