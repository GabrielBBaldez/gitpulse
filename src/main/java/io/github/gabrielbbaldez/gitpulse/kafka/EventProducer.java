package io.github.gabrielbbaldez.gitpulse.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class EventProducer {

    private final KafkaTemplate<String,String> kafkaTemplate;

    public EventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String repoName,String eventJson){
        kafkaTemplate.send("github.events.raw",repoName,eventJson);
    }
}
