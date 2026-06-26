package io.github.gabrielbbaldez.gitpulse.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.gabrielbbaldez.gitpulse.kafka.EventProducer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class GitHubEventPoller {
    private final GitHubClient gitHubClient;
    private final ObjectMapper objectMapper;
    private final EventDedupe eventDedupe;
    private final EventProducer eventProducer;

    public GitHubEventPoller(GitHubClient gitHubClient, ObjectMapper objectMapper, EventDedupe eventDedupe, EventProducer eventProducer) {
        this.gitHubClient = gitHubClient;
        this.objectMapper = objectMapper;
        this.eventDedupe = eventDedupe;
        this.eventProducer = eventProducer;
    }

    @Scheduled(fixedDelay = 60000)
    public void poll() throws Exception {
        String json = gitHubClient.fetchEvents();
        if(json == null){
            System.out.println("304 — nothing new");
            return;
        }
       JsonNode events = objectMapper.readTree(json);

       for (JsonNode eventNode : events){
           String type = eventNode.get("type").asText();
           String repo = eventNode.get("repo").get("name").asText();
           String id = eventNode.get("id").asText();
           eventProducer.publish(repo,eventNode.toString());
           if(eventDedupe.isNew(id)){
               System.out.println(type + " → " + repo);
           }
       }
        System.out.println();
    }
}
