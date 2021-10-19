package producer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class TransfertToHbase {
    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("read file");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Producer producer = new Producer();

        //read and convert file from hdfs
        List<String> list1 = sc.textFile("hdfs://localhost:54310/out/query1")
                .collect();

        List<String> list2 = sc.textFile("hdfs://localhost:54310/out/query2")
                .collect();

        List<String> list3 = sc.textFile("hdfs://localhost:54310/out/query3")
                .collect();

        List<String> list3a = sc.textFile("hdfs://localhost:54310/out/query3a")
                .collect();

        //send to Hbase
        producer.sendToHbase(list1,"hbase1");
        producer.sendToHbase(list2,"hbase1");
        producer.sendToHbase(list3,"hbase1");
        producer.sendToHbase(list3a,"hbase1");
    }
}
