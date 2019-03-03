package top.ljming.javaSparkLearn.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import java.util.Comparator;
import java.util.List;

public class SparkActions {

    public static void sparkActions(JavaRDD<String> javaRDD) {
        javaRDD.map(String::length).foreach(length -> System.out.println(length));

        String longerLog = sparkActionsReduce(javaRDD);
        System.out.println("最长的日志长度: " + longerLog.length());

        List<String> logString = sparkActionsCollect(javaRDD);
        System.out.println("数据集中共有 " + logString.size() + " 个元素");

        System.out.println("数据集中有: " + sparkActionsCount(javaRDD) + " 个元素");

        System.out.println("随机抽样的数据: ");
        sparkTakeSample(javaRDD).forEach(log -> System.out.println(log));

        System.out.println("排序后前 n 个数据");
        sparkTakeOrdered(javaRDD).forEach(log -> System.out.println(log.length()));

        String filePath = "D:\\lijiangming\\docs\\sparkLearn\\";

        sparkActionSaveAsTextFile(javaRDD, filePath);

        sparkActionsSaveAsObjectFile(javaRDD, filePath);
    }

    /**
     * 使用函数 func 聚合数据集中的元素，这个函数 func 输入为两个元素，返回为一个元素。
     * 这个函数应该是可交换（commutative ）和关联（associative）的，这样才能保证它可以被并行地正确计算。
     *
     * @param javaRDD
     * @return
     */
    private static String sparkActionsReduce(JavaRDD<String> javaRDD) {
        return javaRDD.reduce((Function2<String, String, String>) (s, s2) -> {
            if (s.length() > s2.length()) {
                return s;
            } else {
                return s2;
            }
        });
    }

    /**
     * 以 List 形式返回数据集中的所有元素。
     * 通常用于 filter 或者其它返回足够少数据的操作
     *
     * @param javaRDD
     * @return
     */
    private static List<String> sparkActionsCollect(JavaRDD<String> javaRDD) {
        return javaRDD.collect();
    }

    /**
     * 返回数据集中的元素个数
     *
     * @param javaRDD
     * @return
     */
    private static long sparkActionsCount(JavaRDD<String> javaRDD) {
        return javaRDD.count();
    }

    /**
     * 获取数据集中的数据；
     * first() 获取第一个，类似于 take(1)
     * take(int n) 取出前 n 个数。
     *
     * @param javaRDD
     */
    private static void sparkActionsGetItem(JavaRDD<String> javaRDD) {
        String first = javaRDD.first(); // 取出数据集中的第一个元素
        List<String> logList = javaRDD.take(6); // 取出数据集中的前 n 个元素

        System.out.println(first + logList.size());
    }

    /**
     * 对数据集进行随机抽样
     * boolean withReplacement: 是否用随机数进行替换
     * int num: 随机抽取个数
     * long seed: 指定随机数生成器
     *
     * @param javaRDD
     * @return
     */
    private static List<String> sparkTakeSample(JavaRDD<String> javaRDD) {
        return javaRDD.takeSample(true, 5, 3);
    }

    /**
     * 返回数据集中经过排序的前 n 个元素
     *
     * @param javaRDD
     * @return
     */
    private static List<String> sparkTakeOrdered(JavaRDD<String> javaRDD) {
        return javaRDD.takeOrdered(5, Comparator.comparingInt(String::length));
    }

    /**
     * 将数据集中的元素以文本形式保存到指定的本地文件系统、HDFS 或其他 Hadoop 支持的文件系统
     *
     * @param javaRDD
     * @param path
     */
    private static void sparkActionSaveAsTextFile(JavaRDD<String> javaRDD, String path) {
        System.out.println("将数据集中的元素以文本文件的形式写入本地文件系统");
        javaRDD.saveAsTextFile(path + "log.txt");
    }


    private static void sparkActionsSaveAsObjectFile(JavaRDD<String> javaRDD, String path) {
        System.out.println("将数据集中的元素以 Java 序列化的形式写入本地文件系统");
        javaRDD.saveAsObjectFile(path + "log.json"); // 并不是一个 json 格式的文件，而是二进制文件。
    }
}
