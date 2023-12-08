package com.kafka.example.interceptor;

import com.kafka.example.model.User;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class MyConsumerInterceptor  implements ConsumerInterceptor<String, User> {

    @Override
    public ConsumerRecords<String, User> onConsume(ConsumerRecords<String, User> consumerRecords) {
        System.out.println("Interceptor : onConsume()");
        return null;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        System.out.println("Interceptor : oncommit()");
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        System.out.println("Interceptor : onconfigure()");
    }
}
