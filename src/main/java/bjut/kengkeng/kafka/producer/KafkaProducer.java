package bjut.kengkeng.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer {

    private final Producer<String, String> producer;

    public final static String TOPIC = "kafka-1";

    private KafkaProducer() {
        Properties properties = new Properties();

        // kafka集群，brokerlist
        properties.put("bootstrap.servers", "172.17.0.2:9092");

        // 配置value的序列化类
        //properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", StringSerializer.class.getName());

        // 配置key的序列化
        properties.put("key.serializer", StringSerializer.class.getName());

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        properties.put("request.required.acks", "-1");

        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
    }

    void producer() {
        int messageNo = 1000;
        final int COUNT = 100000;
        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
            producer.send(new ProducerRecord<>(TOPIC, key, data));
            System.out.println(data);
            messageNo ++;
        }
    }

    public static void main(String[] args) {
        new KafkaProducer().producer();
    }
}
