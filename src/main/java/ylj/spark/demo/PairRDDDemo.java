/**
 *  @author hzyanglujun
 *  @version  创建时间:2017年3月31日 下午3:15:36
 */
package ylj.spark.demo;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author hzyanglujun
 *
 */
public class PairRDDDemo {
    public static void main(String[] args) {
        // count();
        pairRDD2();
    }

    public static void pairRDD1() {

        List<String> srcData = Arrays.asList("abc", "a", "b", "c");

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddData = sc.parallelize(srcData).cache();

        PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
     
            private static final long serialVersionUID = 3953784456269266132L;

            public Tuple2<String, String> call(String x) {
                return new Tuple2<String, String>(x.split(" ")[0], x);
            }
        };
        
        JavaPairRDD<String, String> pairs = rddData.mapToPair(keyData);

        System.out.println(pairs.count());

    }
    
    //SparkContext.parallelizePairs()。
    public static void pairRDD2() {

        List<String> srcData = Arrays.asList("abc", "a", "b", "c");

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, String>> list=new LinkedList<Tuple2<String, String>>();
        list.add(new Tuple2<String, String>("a","abc"));
        list.add(new Tuple2<String, String>("a","a"));
        list.add(new Tuple2<String, String>("b","b"));
        list.add(new Tuple2<String, String>("c","c"));
        
        
        JavaPairRDD<String,String> rddData = sc.parallelizePairs(list);

       
        System.out.println(rddData.count());

    }
}
