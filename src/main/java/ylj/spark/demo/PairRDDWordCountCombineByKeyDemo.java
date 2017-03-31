/**
 *  @author hzyanglujun
 *  @version  创建时间:2017年3月31日 下午3:34:08
 */
package ylj.spark.demo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author hzyanglujun
 *
 */
public class PairRDDWordCountCombineByKeyDemo {

    public static class AvgCount implements Serializable {
        private static final long serialVersionUID = 1417994962871807952L;

        public AvgCount(int total, int num) {
            total_ = total;
            num_ = num;
        }

        public int total_;
        public int num_;

        public float avg() {
            return total_ / (float) num_;
        }
    }



    public static void main(String[] args) {
        
        Function<Integer, AvgCount> createCombiner = new Function<Integer, AvgCount>() {
            private static final long serialVersionUID = 5752048947903111598L;

            public AvgCount call(Integer x) {
                
                //每个分区 创建一个
                AvgCount aAvgCount= new AvgCount(x, 1);
                
                System.out.println("createCombiner call aAvgCount ->"+aAvgCount.hashCode());
                return aAvgCount;
            }
        };

        Function2<AvgCount, Integer, AvgCount> mergeValue = new Function2<AvgCount, Integer, AvgCount>() {
            private static final long serialVersionUID = 835666279349127098L;

            public AvgCount call(AvgCount a, Integer x) {
                System.out.println("mergeValue call a ->"+a.hashCode());
                
                //分区内进行累加
                a.total_ += x;
                a.num_ += 1;
                return a;
            }
        };
        Function2<AvgCount, AvgCount, AvgCount> mergeCombiners = new Function2<AvgCount, AvgCount, AvgCount>() {
            private static final long serialVersionUID = -443401171742106531L;

            public AvgCount call(AvgCount a, AvgCount b) {
//                AvgCount newAvgCount=new AvgCount();
               
                //合并各个分区
                int total= a.total_ +b.total_;
                int num=a.num_ + b.num_;
                AvgCount m= new AvgCount(total,num);
                System.out.println("mergeCombiners call("+a.hashCode()+","+b.hashCode()+")->"+m.hashCode());
                return m;
            }
        };
        
        List<Tuple2<String, Integer>> nums=new LinkedList<Tuple2<String, Integer>>();
        nums.add(new Tuple2<String, Integer>("a",1));
        nums.add(new Tuple2<String, Integer>("a",3));
        nums.add(new Tuple2<String, Integer>("a",3));
        nums.add(new Tuple2<String, Integer>("b",2));
        nums.add(new Tuple2<String, Integer>("c",1));
        
        
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> numsRDD = sc.parallelizePairs(nums);
    
        //每个分区 创建一个
        
        JavaPairRDD<String, AvgCount> avgCounts =  numsRDD.combineByKey(createCombiner, mergeValue, mergeCombiners);
        Map<String, AvgCount> countMap = avgCounts.collectAsMap();
        for (Entry<String, AvgCount> entry: countMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue().avg());
        }
    }
}
