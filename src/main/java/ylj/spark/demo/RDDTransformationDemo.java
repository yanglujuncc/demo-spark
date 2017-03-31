/**
 *  @author hzyanglujun
 *  @version  创建时间:2017年3月31日 下午1:46:10
 */
package ylj.spark.demo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

/**
 * @author hzyanglujun
 *
 */
public class RDDTransformationDemo {

    public static void main(String[] args) {
        // filter();
//        map();
        flatMap();
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

    public static void flatMap() {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi"));
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 7444865595877233972L;

            public Iterator<String> call(String line) {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        
        for (String word: words.collect()) {
            System.out.println("word:"+word);
        }

       
//        System.out.println(StringUtils.join(words.collect(), ","));
    }
    /*
     * 数据为{1, 2, 3, 3}的RDD进行基本的RDD转化操作
        distinct() 去重 rdd.distinct() {1, 2, 3}
        sample(withReplacement, fraction, [seed])  对 RDD 采样，以及是否替换
     */
    
    
    /*
     * 对数据分别为{1, 2, 3}和{3, 4, 5}的RDD
     * union() 生成一个包含两个 RDD 中所有元素的 RDD rdd.union(other) {1, 2, 3, 3, 4, 5
     * intersection() 求两个 RDD 共同的元素的 RDD rdd.intersection(other) {3}
     * subtract() 移除一个 RDD 中的内容（例如移除训练数据）rdd.subtract(other) {1, 2}
     * cartesian() 与另一个 RDD 的笛卡儿积 rdd.cartesian(other) {(1, 3), (1, 4), ...(3, 5)}
     */
}
