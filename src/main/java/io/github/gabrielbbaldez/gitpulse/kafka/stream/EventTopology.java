package io.github.gabrielbbaldez.gitpulse.kafka.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.Duration;

@Configuration
@EnableKafkaStreams
public class EventTopology {
    private final ObjectMapper mapper = new ObjectMapper();

    @Bean
    public KStream<String, String> eventsStream(StreamsBuilder builder, SimpMessagingTemplate messaging){
        KStream<String,String> events = builder.stream("github.events.raw");
       events
               .groupBy((repo, json) -> typeOf(json))
               .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
               .count()
               .toStream()
               .foreach((windowedType, count) ->
                      messaging.convertAndSend("/topic/stats", new StatSnapshot(windowedType.key(), count)));

        events
                .filter((repo,json) -> {
                    String type = typeOf(json);
                    return type.equals("WatchEvent")
                            || type.equals("ForkEvent")
                            || type.equals("PullRequestEvent");
                })
                .groupBy((repo, json) -> repoOf(json))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .count()
                .toStream()
                .foreach((windowedRepo, count) ->
                      messaging.convertAndSend("/topic/trending", new RepoCount(windowedRepo.key(), count)));

        return events;
    }

    private String typeOf(String json){
        try{
            return mapper.readTree(json).get("type").asText();
        }catch (Exception e){
            return "Unknow";
        }
    }

    private String repoOf(String json){
        try {
            return mapper.readTree(json).get("repo").get("name").asText();
        } catch (Exception e) {
            return "Unknow";
        }
    }
}
