package top.ljming.javaSparkLearn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import top.ljming.javaSparkLearn.action.SparkActions;
import top.ljming.javaSparkLearn.transformation.SparkTransformation;

public class JavaSparkApplication {

    /**
     * 初始化 SparkContext
     *
     * @return
     */
    private static JavaSparkContext initJavaSparkContext() {
        SparkConf conf = new SparkConf().setAppName(JavaSparkApplication.class.getSimpleName()).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    public static void main(String[] args) {

        // spark RDD 的 Transformation 和 action 操作
        JavaSparkContext sc = initJavaSparkContext();

        // 测试 spark transformation 操作
        runSparkTransformation(sc);

        // 测试 spark actions 操作
        runSparkActions(sc);

    }

    private static void runSparkTransformation(JavaSparkContext sc) {
        JavaRDD<String> lines = sc.textFile("D:\\lijiangming\\docs\\sparkLearn\\sparkSQLExample.json");
        SparkTransformation.sparkTransformations(lines);
    }

    private static void runSparkActions(JavaSparkContext sc) {
        JavaRDD<String> lines = sc.textFile("D:\\lijiangming\\docs\\sparkLearn\\sparkSQLExample.json");
        SparkActions.sparkActions(lines);
    }

}
