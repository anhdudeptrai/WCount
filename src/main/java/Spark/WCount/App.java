package Spark.WCount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class App 
{
    public static void main( String[] args ) throws Exception
    {
    	SparkConf conf = new SparkConf();
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	JavaRDD<String> content = sc.textFile("hdfs://192.168.23.202:9000/user/dutd/text");
//    	System.out.println(content.count());
//    	content.coalesce(1).saveAsTextFile("hdfs://192.168.23.203:9000/user/dutd/out1");
    	JavaPairRDD<String, Integer> counts = content
    		    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
    		    .mapToPair(word -> new Tuple2<>(word, 1))
    		    .reduceByKey((a, b) -> a + b);
//    	System.out.println(counts.collect());
    	
    	counts.coalesce(1).saveAsTextFile("hdfs://192.168.23.202:9000/user/dutd/out1");
    }
}
