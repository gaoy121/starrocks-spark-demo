package com.starrocks.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.sql.SQLException;
import java.util.Properties;

/**
 * @auther GaoYuan
 * @date 2022/2/9 10:02
 * @desc
 */
public class Mysql2StarRocks {

    // parameters
    private static String master = "local[2]";
    private static String appName = "app_spark_demo";

    private static String srcHost = "localhost";
    private static Integer srcPort = 3306;
    private static String srcUserName = "root";
    private static String srcPassword = "root";
    private static String srcDataBase = "test";
    private static String srcTable = "user";

    private static String destHost = "192.168.110.101";
    private static Integer destPort = 8040;
    private static String destUserName = "root";
    private static String destPassowrd = "root";
    private static String destDataBase = "gyuan";
    private static String destTable = "users";

    private static Double filterRatio =  0.2;
    private static String columns = "uid,date,hour,minute,site";

    public static void main(String[] args) throws SQLException {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName(appName).setMaster(master));
        SQLContext sqlContext = new SQLContext(sparkContext);

        //jdbc.url=jdbc:mysql://localhost:3306/database
        String url = "jdbc:mysql://" + srcHost + ":" + srcPort + "/" + srcDataBase + "?characterEncoding=utf-8&autoReconnect=true&zeroDateTimeBehavior=convertToNull&useSSL=false&serverTimezone=Asia/Shanghai";
        //查找的表名
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user",srcUserName);
        connectionProperties.put("password", srcPassword);
        connectionProperties.put("driver","com.mysql.cj.jdbc.Driver");

        //SparkJdbc读取表内容
        System.out.println("读取test数据库中的user表内容");
        // 读取表中所有数据
        Dataset<Row> rowDataset = sqlContext.read().jdbc(url,srcTable,connectionProperties).select("*");
        //显示数据
        //rowDataset.show();

        rowDataset.write().format("starrocks")
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
                //.option("doris.write.fields","$YOUR_FIELDS_TO_WRITE")
                .option("starrocks.max.filter.ratio",filterRatio)
                .save();
    }

}
