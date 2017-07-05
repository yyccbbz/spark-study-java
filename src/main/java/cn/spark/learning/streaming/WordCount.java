package cn.spark.learning.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-5
 * @Time: 16:33
 * @Description: 实时WordCount程序
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WordCount");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));

        //创建输入DStream
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 999);



        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
