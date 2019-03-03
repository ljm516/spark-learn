package top.ljming.javaSparkLearn.transformation;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import top.ljming.javaSparkLearn.common.LearnUtils;
import top.ljming.javaSparkLearn.model.Person;

import java.util.List;

public class SparkTransformation {

    public static void sparkTransformations(JavaRDD<String> lines) {

        mapTransformation(lines).foreach(lineLength -> System.out.println(lineLength));

        JavaRDD<Person> logJavaRDD = lines.map((Function<String, Person>) s -> {
            JSONObject jObj = JSONObject.parseObject(s);
            return LearnUtils.buildPerson(jObj);
        });

        JavaPairRDD<String, Person> pairJavaRDD = pairRddTransformation(logJavaRDD);
        pairJavaRDD.foreach(pairJavaRdd -> System.out.println(pairJavaRdd));

        JavaPairRDD<String, Iterable<Person>> key2ListPairRDD = groupPairRddByKey(pairJavaRDD);
                    key2ListPairRDD.foreach(tuple -> {
            System.out.println("key: " + tuple._1());
            tuple._2().forEach(Person -> System.out.println(Person));
        });

        JavaPairRDD<String, Person> reducedPairJavaRDD = reduceByKey(pairJavaRDD);
        reducedPairJavaRDD.foreach(tuple -> System.out.println(tuple._1() + tuple._2()));

        List<Tuple2<String, Person>> sortedPairRDDList = sortByKey(pairJavaRDD);
        sortedPairRDDList.forEach(tuple -> System.out.println(tuple._1() + tuple._2()));
    }

    /**
     * map 转化：将数据集的每个元素按照指定的函数转换成一个新的 RDD
     *
     * @param lines
     */
    private static JavaRDD<Integer> mapTransformation(JavaRDD<String> lines) {
        return lines.map(String::length);
    }

    /**
     * 将 logJavaRdd 做一个<k, v> 映射
     * @param logJavaRDD
     * @return
     */
    private static JavaPairRDD<String, Person> pairRddTransformation(JavaRDD<Person> logJavaRDD) {
        return logJavaRDD.mapToPair(Person -> {
            return new Tuple2<>(Person.getName(), Person);
        });
    }

    /**
     * 对有相同 key 的元素进行分组操作
     * 将 pairJavaRDD 按 K 分组
     */
    private static JavaPairRDD<String, Iterable<Person>> groupPairRddByKey(JavaPairRDD<String, Person> pairJavaRDD) {
        JavaPairRDD<String, Iterable<Person>> key2ListPairRDD = pairJavaRDD.groupByKey();
        return key2ListPairRDD;
    }

    /**
     * 对有相同 key 的元素根据指定的函数进行聚合操作，返回 JavaPairRDD<K, V>
     * @param pairJavaRDD
     * @return
     */
    private static JavaPairRDD<String, Person> reduceByKey(JavaPairRDD<String, Person> pairJavaRDD) {
        return pairJavaRDD.reduceByKey((Function2<Person, Person, Person>) (Person, Person2) -> Person);
    }

    /**
     * 根据 key 排序，并返回最终结果 List<Tuple2<K, V>>
     * @param pairJavaRDD
     * @return
     */
    private static List<Tuple2<String, Person>> sortByKey(JavaPairRDD<String, Person> pairJavaRDD) {
        return pairJavaRDD.sortByKey().collect();
    }
}
