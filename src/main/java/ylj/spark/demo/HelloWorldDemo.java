package ylj.spark.demo;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class HelloWorldDemo {

    public static void main(String[] args) {

        List<String> srcData = Arrays.asList("abc", "a", "b", "c");

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> logData = sc.parallelize(srcData).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {

            /**
            	 * 
            	 */
            private static final long serialVersionUID = 5381930222098233051L;

            public Boolean call(String s) {

                return s.contains("a");

            }

        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {

            /**
            	 * 
            	 */
            private static final long serialVersionUID = 4106701822571019042L;

            public Boolean call(String s) {

                return s.contains("b");

            }

        }).count();

        long count = logData.count();
        
        

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs + ", count = " + count);

        sc.stop();
    }
}
