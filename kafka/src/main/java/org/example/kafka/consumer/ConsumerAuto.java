package org.example.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerAuto {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "my-consumer");
        // 오토 커밋 사용
        props.put("enabla.auto.commit", "true");
        // 오프셋을 찾지 못하는 경우 latest로 초기화하며 가장 최근부터 메시지를 가져옴
        props.put("auto.offset.reset", "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 구독할 토픽을 지정
        consumer.subscribe(List.of("my-topic"));
        try {
            while (true) {
                // 타임아웃 주기를 설정. 컨슈머는 해당 시간만큼 블록함
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("records : " + record.value());
                }
            }
        } catch (Exception e) {
            log.error("exception");
        }

    }
}
