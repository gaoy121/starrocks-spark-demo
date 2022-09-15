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

/**
 * @auther GaoYuan
 * @date 2022/9/15 13:20
 * @desc
 */
public class StarRocksRead {

    // parameters
    private static String master = "local[2]";
    private static String appName = "app_spark_demo";

    private static String srcHost = "192.168.110.101";
    private static Integer srcPort = 8030;
    private static String srcUserName = "root";
    private static String srcPassword = "root";
    private static String srcDataBase = "gyuan";
    private static String srcTable = "users";

    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName(appName).setMaster(master));
        SQLContext sqlContext = new SQLContext(sparkContext);

        Dataset<Row> dataset = sqlContext.read().format("starrocks")
                .option("starrocks.table.identifier", srcDataBase + "." + srcTable)
                .option("starrocks.fenodes", srcHost + ":" + srcPort)
                .option("user", srcUserName)
                .option("password", srcPassword)
                .load();

        dataset.show();
    }

}
