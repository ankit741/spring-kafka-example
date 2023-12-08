package com.kafka.example.listener;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Collection;

/**
 * However, there is still a chance that our application will crash after
 * the record was stored in the database but before we committed offsets,
 * causing the record to be processed again and the database to contain duplicates.
 */
@Component
public class KafkaRebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        System.out.println("*************************onPartitionsRevoked*****************");
        // commitDBTransaction(); 1
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        System.out.println("*************************onPartitionsAssigned*****************");
        /*
           for(TopicPartition partition: partitions)
            consumer.seek(partition, getOffsetFromDB(partition)); 2
    }
         */
    }
}
