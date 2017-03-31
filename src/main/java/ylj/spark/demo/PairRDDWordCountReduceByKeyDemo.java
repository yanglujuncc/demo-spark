/**
 *  @author hzyanglujun
 *  @version  创建时间:2017年3月31日 下午3:34:08
 */
package ylj.spark.demo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author hzyanglujun
 *
 */
public class PairRDDWordCountReduceByKeyDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> srcData = Arrays.asList("abc ddd", "a b", "b abc", "c b d");

        JavaRDD<String> lines=sc.parallelize(srcData);
        
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 7444865595877233972L;
            public Iterator<String> call(String line) {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        
        JavaPairRDD<String, Integer> result = words.mapToPair(new PairFunction<String, String, Integer>() {
           
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Integer> call(String x) {
                return new Tuple2<String, Integer> (x, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
         
            private static final long serialVersionUID = 8403867309942667296L;

            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        
        
        for (Tuple2<String, Integer> wordCount: result.collect()) {
            System.out.println("word:"+wordCount);
        }


    }
}
