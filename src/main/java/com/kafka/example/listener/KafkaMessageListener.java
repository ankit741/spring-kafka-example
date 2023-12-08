package com.kafka.example.listener;

import com.kafka.example.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;


@Component
public class KafkaMessageListener implements ConsumerSeekAware {

    private final ThreadLocal<ConsumerSeekCallback> seekCallBack = new ThreadLocal<>();

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        this.seekCallBack.set(callback);
    }

    @KafkaListener(id = "kafka1",topics = "kafka-example", concurrency = "1", groupId = "group_id",containerFactory = "akafkaListenerContainerFactory")
    public void consume(@Payload User user, @Headers MessageHeaders messageHeaders, Acknowledgment ack) {
        System.out.println("thread name:"+Thread.currentThread().getName());
        System.out.println(user.getName());
        if (user.getName().equals("ankit")) {
            System.out.println("processing same message again and again");
            this.seekCallBack.get().seek("kafka-example", (int)(messageHeaders.get("kafka_receivedPartitionId")), (long)(messageHeaders.get("kafka_offset")));
        }

        try {
            /*Business logic*/
            ack.acknowledge();
        } catch (Exception e) {
            throw new RuntimeException("Message exception, enter the dead letter queue...");
        }

    }

    /**
     *         long limit = record.timestamp() + TimeUnit.MINUTES.toMillis(5);
     *         if (System.currentTimeMillis() < limit) {
     *             consumer.pause(consumer.partitionsFor(record.topic()));
     *             while (System.currentTimeMillis() < limit) {
     *                 long sleep = Math.min(limit - System.currentTimeMillis(), 60000);
     *                 Thread.sleep(sleep);
     *                 consumer.poll(0L);
     *             }
     *             consumer.resume(consumer.partitionsFor(record.topic()));
     *         }
     */

}
