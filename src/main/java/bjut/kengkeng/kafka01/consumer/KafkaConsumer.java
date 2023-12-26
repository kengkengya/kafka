package bjut.kengkeng.kafka01.consumer;

import bjut.kengkeng.kafka01.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaConsumer {

    private final org.apache.kafka.clients.consumer.KafkaConsumer consumer;

    private KafkaConsumer() {

        Properties props = new Properties();

        props.put("bootstrap.servers", "172.17.0.2:9092,172.17.0.3:9093,172.17.0.4:9094");
        props.put("group.id", "0");
        props.put("max.poll.records", 100);
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);
    }

    void consume() {

        consumer.subscribe(Arrays.asList(KafkaProducer.TOPIC));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                // 入库操作
                //insertIntoDb(buffer);
                for (ConsumerRecord<String, String> record : buffer) {
                    System.out.println(record.key() + "-> " + record.value());
                }

                consumer.commitSync();
                buffer.clear();
            }
        }
    }

    public static void main(String[] args) {
        new KafkaConsumer().consume();
    }
}
