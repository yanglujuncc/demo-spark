/**
 *  @author hzyanglujun
 *  @version  创建时间:2017年3月31日 下午1:46:19
 */
package ylj.spark.demo;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * @author hzyanglujun
 *
 */
public class RDDActionDemo {
    public static void main(String[] args) {
//        count();
        take();
    }

    public static void count() {

        List<String> srcData = Arrays.asList("abc", "a", "b", "c");

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddData = sc.parallelize(srcData).cache();

        System.out.println(rddData.count());

    }

    public static void take() {

        List<String> srcData = Arrays.asList("abc", "a", "b", "c");

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddData = sc.parallelize(srcData).cache();

//        System.out.println(rddData.count());

        //使用 take() 获取了 RDD 中的少量元素 取回本地
        for (String line: rddData.take(2)) {
            System.out.println("line:"+line);
        }
    }
    
    
    public static void collect(){
        List<String> srcData = Arrays.asList("abc", "a", "b", "c");

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddData = sc.parallelize(srcData).cache();

//        System.out.println(rddData.count());

        //可以用来获取整个 RDD 中的数据,在大多数情况下， RDD 不能通过 collect() 收集到驱动器进程中，因为它们一般都很大,我们通常要把数据写到诸如 HDFS 或 Amazon S3 这样的分布式的存储系统中
        //可 以使用 saveAsTextFile()、 saveAsSequenceFile()
        for (String line: rddData.collect()) {
            System.out.println("line:"+line);
        }
    }
}
