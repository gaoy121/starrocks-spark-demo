package com.starrocks.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * @auther GaoYuan
 * @date 2022/2/9 10:02
 * @desc
 */
public class StarRocks2StarRocks {

    // parameters
    private static String master = "local[2]";
    private static String appName = "app_spark_demo";

    private static String srcHost = "192.168.110.101";
    private static Integer srcPort = 8030;
    private static String srcUserName = "root";
    private static String srcPassword = "root";
    private static String srcDataBase = "gyuan";
    private static String srcTable = "gy_test";

    private static String destHost = "192.168.110.101";
    private static Integer destPort = 8040;
    private static String destUserName = "root";
    private static String destPassowrd = "root";
    private static String destDataBase = "gyuan";
    private static String destTable = "gy_test";

    private static Double filterRatio =  0.2;
    private static String columns = "uid,date,hour,minute,site";

    public static void main(String[] args) throws SQLException, TimeoutException {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName(appName).setMaster(master));
        SQLContext sqlContext = new SQLContext(sparkContext);

        Dataset<Row> dataset = sqlContext.read().format("starrocks")
                .option("starrocks.table.identifier", srcDataBase + "." + srcTable)
                .option("starrocks.fenodes", srcHost + ":" + srcPort)
                .option("user", srcUserName)
                .option("password", srcPassword)
                .load();

        dataset.write().format("starrocks")
                .option("starrocks.benodes",destHost + ":" + destPort)
                .option("starrocks.table.identifier", destDataBase + "." + destTable)
                //写入的重试次数，默认1次
                .option("starrocks.sink.max-retries", 3)
                //每次写入的数据条数，默认10000
                .option("starrocks.sink.batch.size", 10000)
                .option("user", destUserName)
                .option("password", destPassowrd)
                //其它选项
                //指定你要写入的字段
                //.option("starrocks.write.fields","$YOUR_FIELDS_TO_WRITE")
                .option("starrocks.max.filter.ratio",filterRatio)
                .save();
    }

}
