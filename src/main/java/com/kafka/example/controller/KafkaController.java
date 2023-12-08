package com.kafka.example.controller;

import com.kafka.example.model.User;
import java.util.Objects;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public final class KafkaController {

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @PostMapping("/produce")
    public void sendMessage(@RequestBody User user) {
        System.out.println(user);
        Message<User> message = MessageBuilder
                .withPayload(user)
                .setHeader(KafkaHeaders.TOPIC, "kafka-example")
                .setHeader(KafkaHeaders.MESSAGE_KEY, String.valueOf(UUID.randomUUID()))
                .build();
        this.kafkaTemplate.send(message);
        //this.kafkaTemplate.send("kafka-example", String.valueOf(UUID.randomUUID()),user);
    }

    @GetMapping("/info")
    public Object getConsumerIds() {
        System.out.println(kafkaListenerEndpointRegistry.getListenerContainerIds().toString());
        return kafkaListenerEndpointRegistry.getListenerContainerIds();

    }


    @GetMapping("/pause")
    public void pauseKafkaConsumers(@RequestParam String consumerId) {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId);
        if (Objects.isNull(listenerContainer)) {
            throw new RuntimeException(String.format("Consumer with id %s is not found", consumerId));
        } else if (!listenerContainer.isRunning()) {
            throw new RuntimeException(String.format("Consumer with id %s is not running", consumerId));
        } else if (listenerContainer.isContainerPaused()) {
            throw new RuntimeException(String.format("Consumer with id %s is already paused", consumerId));
        } else if (listenerContainer.isPauseRequested()) {
            throw new RuntimeException(String.format("Consumer with id %s is already requested to be paused", consumerId));
        } else {
            listenerContainer.pause();
          /*  publishMessage(ConsumerActionRequest.builder()
                    .consumerId(consumerId)
                    .consumerAction(ConsumerAction.PAUSE)
                    .build());*/
        }
    }

    @GetMapping("/resume")
    public void resumeConsumer(@RequestParam String consumerId) {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId);
        if (Objects.isNull(listenerContainer)) {
            throw new RuntimeException(String.format("Consumer with id %s is not found", consumerId));
        } else if (!listenerContainer.isRunning()) {
            throw new RuntimeException(String.format("Consumer with id %s is not running", consumerId));
        } else if (!listenerContainer.isContainerPaused()) {
            throw new RuntimeException(String.format("Consumer with id %s is not paused", consumerId));
        } else {
            listenerContainer.start();
      /*      publishMessage(ConsumerActionRequest.builder()
                    .consumerId(consumerId)
                    .consumerAction(ConsumerAction.RESUME)
                    .build());*/
        }
    }

}