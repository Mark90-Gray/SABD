package producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Producer {

    private static void sendMessage(org.apache.kafka.clients.producer.Producer<String, String> producer, String key, String value, String topic){
        try {
            producer.send(
                    new ProducerRecord<String, String>(topic, key, value))
                    .get();
            //System.out.println("Sent message: (" + key + ", " + value + ")");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
    //send to HDFS
    public static void sendToHDFS(List<String> list, String topic) {

        org.apache.kafka.clients.producer.Producer<String, String> producer = ProducerCreator.createProducer();

        try {
            int i;
            for(i=0; i<list.size(); i++){
                String row = list.get(i);
                sendMessage(producer,i+"", row, topic);
            }
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    //send to Hbase
    public static void sendToHbase(List<String> list, String topic) {

        org.apache.kafka.clients.producer.Producer<String, String> producer = ProducerCreator.createProducer();

        try {
            int i;
            for(i=0; i<list.size(); i++){
                String row = list.get(i);
                sendMessage(producer,i+"", row, topic);
            }
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    //send to HDFS and Hbase
    public static void sendToConsumers(List<String> list, String topic) {

        org.apache.kafka.clients.producer.Producer<String, String> producer = ProducerCreator.createProducer();

        try {
            int i;
            for(i=0; i<list.size(); i++){
                String row = list.get(i);
                sendMessage(producer,i+"", row, topic);
            }
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

}
