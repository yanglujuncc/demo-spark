/**
 *  @author hzyanglujun
 *  @version  创建时间:2017年3月31日 下午3:24:48
 */
package ylj.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author hzyanglujun
 *
 */
public class PairRDDTransformationDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        
        
    }
}
