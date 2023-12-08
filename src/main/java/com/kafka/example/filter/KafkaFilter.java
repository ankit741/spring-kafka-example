package com.kafka.example.filter;

import com.kafka.example.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaFilter implements RecordFilterStrategy<String, User> {

    private static final ThreadLocal<byte[]> ids = new ThreadLocal<>();

    @Override
    public List<ConsumerRecord<String, User>> filterBatch(List<ConsumerRecord<String, User>> consumerRecords) {
        return RecordFilterStrategy.super.filterBatch(consumerRecords);
    }

    public static void setId(String id) {
        ids.set(id.getBytes());
    }

    public static void clearId() {
        ids.remove();
    }

    //Return true if the record should be discarded.
    @Override
    public boolean filter(ConsumerRecord<String, User> consumerRecord) {
        System.out.println("*************inside filter**************");
        //Header which = consumerRecord.headers().lastHeader("which");
        //return which == null || !Arrays.equals(which.value(), ids.get());
        return false;
    }
}