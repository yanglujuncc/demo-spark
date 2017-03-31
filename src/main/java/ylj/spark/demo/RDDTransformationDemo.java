/**
 *  @author hzyanglujun
 *  @version  创建时间:2017年3月31日 下午1:46:10
 */
package ylj.spark.demo;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @author hzyanglujun
 *
 */
public class RDDTransformationDemo {

    public static void main(String[] args) {
//        filter();
        map();
    }

    public static void filter() {

        List<String> srcData = Arrays.asList("abc", "a", "b", "c");

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddData = sc.parallelize(srcData).cache();

        JavaRDD<String> rddDataFilted = rddData.filter(new Function<String, Boolean>() {

            private static final long serialVersionUID = 8636293203424685985L;

            @Override
            public Boolean call(String paramT1) throws Exception {
                if (paramT1.contains("a")) {
                    return true;
                } else {
                    return false;
                }
            }

        });

        System.out.println(rddDataFilted.count());

    }

    public static void map() {

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer x) {
                return x * x;
            }
        });
        System.out.println(StringUtils.join(result.collect(), ","));

    }
}
