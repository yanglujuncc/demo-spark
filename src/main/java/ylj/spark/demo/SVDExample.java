package ylj.spark.demo;

import java.util.LinkedList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

public class SVDExample {

    public static void main(String[] args) {

    }

    public static void tes1t() {

        // double[][] array = { { 1.12, 2.05, 3.12 }, { 5.56, 6.28, 8.94 }, { 10.2, 8.0, 20.5 } };
        // LinkedList<Vector> rowsList = new LinkedList<Vector>();
        // for (int i = 0; i < array.length; i++) {
        // Vector currentRow = Vectors.dense(array[i]);
        // rowsList.add(currentRow);
        // }
        // JavaRDD<Vector> rows = jsc.parallelize(rowsList);
        //
        // // Create a RowMatrix from JavaRDD<Vector>.
        // RowMatrix mat = new RowMatrix(rows.rdd());
        //
        // // Compute the top 3 singular values and corresponding singular vectors.
        // SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(3, true, 1.0E-9d);
        // RowMatrix U = svd.U();
        // Vector s = svd.s();
        // Matrix V = svd.V();
    }

    public static void test2() {
        SparkConf conf = new SparkConf().setAppName("SVDTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile("/home/yurnom/data/svd.txt");
        JavaRDD<Vector> rows = null;

        RowMatrix mat = new RowMatrix(rows.rdd());
        // 第一个参数3意味着取top 3个奇异值，第二个参数true意味着计算矩阵U，第三个参数意味小于1.0E-9d的奇异值将被抛弃
        SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(3, true, 1.0E-9d);
        RowMatrix U = svd.U(); // 矩阵U
        Vector s = svd.s(); // 奇异值
        Matrix V = svd.V(); // 矩阵V
        System.out.println(s);
        System.out.println("-------------------");
        System.out.println(V);
    }
}
