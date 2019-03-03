package top.ljming.javaSparkLearn.sparkSQL;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import top.ljming.javaSparkLearn.model.Record;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SparkHiveApplication {

    private static final String BASE_DIR = "D:\\lijiangming\\docs\\sparkLearn\\";

    private static SparkSession initSparkSession(String warehouseLocation) {
        return SparkSession.builder().appName(SparkHiveApplication.class.getSimpleName())
                .config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate();
    }

    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        System.out.println("warehouseLocation: ------>" + warehouseLocation);
        SparkSession sparkSession = initSparkSession(warehouseLocation);
        hiveTable(sparkSession);
    }

    // HiveTable
    public static void hiveTable(SparkSession sparkSession) {
        sparkSession.sql("create table if not exists src (key int, value string) using hive");
        try {
            sparkSession.sql("load data local inpath 'kv1.txt' into table src");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.toString());
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        // 查询以 hiveQL 表示
        sparkSession.sql("select * from src").show();

        // 也支持聚合查询
        sparkSession.sql("select count(*) from src").show();

        Dataset<Row> sqlDF = sparkSession.sql("select key, value from src where key < 10 order by key");
        Dataset<String> stringDF = sqlDF.map((MapFunction<Row, String>) row -> "key: " + row.get(0)
                + ", value: " + row.get(1), Encoders.STRING());
        stringDF.show();

        // 也可以结合 SparkSession 使用 DataFrame 去创建临时视图
        List<Record> recordList = new ArrayList<>();
        for(int key = 1; key < 100; key++) {
            Record record = new Record();
            record.setKey(key);
            record.setValue("val_" + key);
            recordList.add(record);
        }
        Dataset<Row> recordDF = sparkSession.createDataFrame(recordList, Record.class);
        recordDF.createOrReplaceTempView("record");

        // 然后查询可以将 DataFrame 数据与 Hive 中的数据结合起来。
        sparkSession.sql("select * from record r join src s on r.key = s.key").show();
    }

    // JDBC to other databases
    public static void jdbc2OtherDatabases(SparkSession sparkSession) {
        Dataset<Row> jdbcDF1 = sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql:dbserver")
                .option("dbtable", "schema.tablename")
                .option("user", "username")
                .option("password", "password")
                .load();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "username");
        connectionProperties.put("password", "pwd");

        Dataset<Row> jdbcDF2 = sparkSession.read().jdbc("jdbc.postgresql:dbserver", "schema.tablename", connectionProperties);

        // 保存数据到 JDBC 源
        jdbcDF1.write()
                .format("jdbc")
                .option("url", "jdbc:postgresql:dbserver")
                .option("datable", "schema.tablename")
                .option("user", "username")
                .option("password", "password")
                .save();

        jdbcDF2.write()
                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

        // 写入数据时指定 table 列的数据类型
        jdbcDF1.write()
                .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

    }
}
