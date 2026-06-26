package io.github.gabrielbbaldez.gitpulse.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
public class Broadcaster {

    private final SimpMessagingTemplate messagingTemplate;

    public Broadcaster(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    @KafkaListener(topics = "github.events.raw", groupId = "gitpulse-broadcaster")
    public void onEvent(String eventJson) {
        messagingTemplate.convertAndSend("/topic/feed", eventJson);
    }
}
