package io.github.gabrielbbaldez.gitpulse.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class GitHubEventPoller {
    private final GitHubClient gitHubClient;
    private final ObjectMapper objectMapper;
    private final EventDedupe eventDedupe;

    public GitHubEventPoller(GitHubClient gitHubClient, ObjectMapper objectMapper, EventDedupe eventDedupe) {
        this.gitHubClient = gitHubClient;
        this.objectMapper = objectMapper;
        this.eventDedupe = eventDedupe;
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
           if(eventDedupe.isNew(id)){
               System.out.println(type + " → " + repo);
           }
       }
        System.out.println();
    }
}
