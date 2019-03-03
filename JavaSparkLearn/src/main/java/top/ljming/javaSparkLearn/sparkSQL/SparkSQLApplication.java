package top.ljming.javaSparkLearn.sparkSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import top.ljming.javaSparkLearn.common.LearnUtils;
import top.ljming.javaSparkLearn.model.Cube;
import top.ljming.javaSparkLearn.model.Person;
import top.ljming.javaSparkLearn.model.Square;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SparkSQLApplication {

    private static final String BASE_DIR = "D:\\lijiangming\\docs\\sparkLearn\\";

    private static SparkSession initSparkSession() {
        return SparkSession.builder().appName(SparkSQLApplication.class.getSimpleName()).master("local").getOrCreate();
    }

    public static void main(String[] args) {
        SparkSession sparkSession = initSparkSession();
        // 以 本地文件创建一个 dataSet
//        Dataset<Row> df = sparkSession.read().json("D:\\lijiangming\\docs\\sparkLearn\\sparkSQLExample.json");

        // spark sql 里一些基础操作
//        dataFrameActions(df);

//        System.out.println("执行 selectBySQL 方法...");
//        selectBySQL(df, sparkSession);
//
//        System.out.println("执行 globalTempView 方法...");
//        globalTempView(df, sparkSession);
//
//        System.out.println("执行 create dataSet 方法...");
//        createDatasets(sparkSession);

//        System.out.println("执行 inferSchemaByReflect 方法...");
//        inferSchemaByReflect(sparkSession);
//
//        System.out.println("执行 inferSchemaByProgram 方法...");
//        inferSchemaByProgram(sparkSession);

        System.out.println("执行 loadingDataProgram method ....");
        loadingDataProgram(sparkSession);

        System.out.println("执行 schemaMerging method ...");
        schemaMerging(sparkSession);
        sparkSession.stop();
    }

    // dataSet 的一些基础操作
    private static void dataFrameActions(Dataset df) {
        df.show();
        // 以树形结构打印 schema
        df.printSchema();

        // 选择 `name` 列
        System.out.println("df.show() second call");
        df.show();
        df.select("name").show();

        // 选择所有的数据，但对 `age` 列执行 +1
        System.out.println("df.show() third call");
        df.show();
        df.select(col("name"), col("age").plus(1)).show();

        // 选择 `age` 大于 21 的 people
        df.select(col("age").gt(21)).show();

        // 根据 `age` 分组并计算
        df.groupBy("age").count().show();

    }

    // select by sql
    private static void selectBySQL(Dataset df, SparkSession sparkSession) {
        df.createOrReplaceTempView("people"); // 注册 DataFrame 为一个 SQL 的临时视图

        Dataset<Row> sqlDF = sparkSession.sql("select * from people");

        sqlDF.show();

    }

    // global temporary view
    private static void globalTempView(Dataset df, SparkSession sparkSession) {
        try {
            // 注册 DataFrame 作为一个全局临时视图
            df.createGlobalTempView("people");
            // 全局临时视图被绑定到系统保留的数据库'global_temp'
            sparkSession.sql("select * from global_temp.people").show();

            // 全局临时视图是跨 session 域的
            sparkSession.newSession().sql("select * from global_temp.people").show();
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    // 创建 datasets
    private static void createDatasets(SparkSession sparkSession) {
        Person person = new Person();
        person.setAge(25);
        person.setCompany("任子行");
        person.setGender("female");
        person.setAddress("wuhan");
        person.setName("chenyang");

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = sparkSession.createDataset(Collections.singletonList(person), personEncoder);
        javaBeanDS.show();

        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = sparkSession.createDataset(Arrays.asList(1, 2, 3, 4, 5), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1, integerEncoder);
        System.out.println(transformedDS.collect());

        String path = BASE_DIR + "sparkSQLExample.json";
        Dataset<Person> peopleDS = sparkSession.read().json(path).as(personEncoder);
        peopleDS.show();

    }

    /**
     * Datasets 和 RDD 相互转换
     */

    // 使用反射推断 Scheme
    public static void inferSchemaByReflect(SparkSession sparkSession) {
        String path = BASE_DIR + "sparkSQLExample.txt";
        // 从一个 txt 文件中创建 RDD
        JavaRDD<Person> personJavaRDD = sparkSession.read().textFile(path).javaRDD().map(line -> LearnUtils.buildPerson(line));

        //  将 schema 应用于 Javabeans 的 rdd 以获取 datasets
        Dataset<Row> personDF = sparkSession.createDataFrame(personJavaRDD, Person.class);

        // 将 DataFrame 注册为一个临时视图
        personDF.createOrReplaceTempView("person");

        // SQL 可以通过 Spark 提供的 sql 方法来执行
        Dataset<Row> selectPersonDF = sparkSession.sql("select name from person where age between 20 and 28");

        // 每一行的每一列数据，可以通过下标的方式
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> selectPersonNameByIndexDF = selectPersonDF.map((MapFunction<Row, String>) row ->
                "name: " + row.<String>getAs("name"), stringEncoder);

        selectPersonNameByIndexDF.show();
    }

    // 使用编程的方式指定 schema
    public static void inferSchemaByProgram(SparkSession sparkSession) {
        String path = BASE_DIR + "sparkSQLExample.txt";
        JavaRDD<String> personRDD = sparkSession.sparkContext().textFile(path, 1).toJavaRDD();

        String schemaString = "name age";

        // 基于 schemaString 生成 schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // 将 rdd 的结果转成 Row
        JavaRDD<Row> rowRdd = personRDD.map((Function<String, Row>) s -> {
            String[] attributes = s.split("\t");
            return RowFactory.create(attributes[0], attributes[4].trim());
        });

        // 将 schema 应用到 rdd
        Dataset<Row> personDataFrame = sparkSession.createDataFrame(rowRdd, schema);

        personDataFrame.createOrReplaceTempView("person");

        Dataset<Row> results = sparkSession.sql("select name from person");

        // SQL 查询的结果是 DataFrame，并支持所有正常的 rdd 操作
        Dataset<String> namesDS = results.map((MapFunction<Row, String>) row -> "name: " + row.getString(0), Encoders.STRING());

        namesDS.show();
    }

    /**
     * DataSource：数据源
     */
    // 程序加载数据
    public static void loadingDataProgram(SparkSession sparkSession) {
        String path = BASE_DIR + "sparkSQLExample.json";
        Dataset<Row> personDF = sparkSession.read().json(path);
        // dataframes 可以保存为 parquet 文件，维护 schema 信息
        personDF.write().parquet(BASE_DIR + "person.parquet");
        // 读取 parquet 文件
        Dataset<Row> parquetFileDF = sparkSession.read().parquet(BASE_DIR + "person.parquet");

        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> namesDf = sparkSession.sql("select name from parquetFile where age between 20 and 28");
        Dataset<String> namesDS = namesDf.map((MapFunction<Row, String>) row -> "name: " + row.getString(0), Encoders.STRING());

        namesDS.show();
    }

    // Schema 合并
    public static void schemaMerging(SparkSession sparkSession) {
        List<Square> squares = new ArrayList<>();
        Arrays.asList(1, 2, 3, 4, 5).stream().forEach(value -> {
            Square square = new Square();
            square.setValue(value);
            square.setSquare(value * value);
            squares.add(square);
        });

        // 创建一个简单的 DataFrame，存储到分区文件夹
        Dataset<Row> squareDF = sparkSession.createDataFrame(squares, Square.class);
        squareDF.write().parquet(BASE_DIR + "data/test_table/key=1");

        List<Cube> cubes = new ArrayList<>();
        Arrays.asList(6, 7, 8, 9, 10).stream().forEach(value -> {
            Cube cube = new Cube();
            cube.setValue(value);
            cube.setValue(value * value * value);
            cubes.add(cube);
        });

        // 在另一个分区文件夹中创建另一个 DataFrame；添加一个新 column ，移除掉一个存在的column
        Dataset<Row> cubeDF = sparkSession.createDataFrame(cubes, Cube.class);
        cubeDF.write().parquet(BASE_DIR + "data/test_table/key=2");

        // 读取分区表
        Dataset<Row> mergedDF = sparkSession.read().option("mergeSchema", true).parquet(BASE_DIR + "data/test_table");
        mergedDF.printSchema();
    }

    // JSON dataset
    public static void JSONDataset(SparkSession sparkSession) {

        // 文件路径可以是单个的文本文件，也可以是一个存储了文本文件的文件夹
        Dataset<Row> personDF = sparkSession.read().json(BASE_DIR + "sparkSQLExample.json");

        // 可推测的 schema 通过 printSchema() 方法显示出来
        personDF.printSchema();

        personDF.createOrReplaceTempView("person");

        Dataset<Row> namesDF = sparkSession.sql("select name from person where age between 20 and 28");
        namesDF.show();

        // 或者，可以通过一个 JSON格式的字符串创建一个 DataFrame
        String jsonData = "{\"name\":\"tracy\",\"address\":\"Houston\",\"gender\":\"male\",\"company\":\"Amzon\",\"age\":\"32\"}";
        Dataset<Row> anotherPerson = sparkSession.read().json(jsonData.toString());
        anotherPerson.show();
    }

}


