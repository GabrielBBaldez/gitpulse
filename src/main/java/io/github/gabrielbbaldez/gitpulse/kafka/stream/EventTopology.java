package io.github.gabrielbbaldez.gitpulse.kafka.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.sql.SQLOutput;
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

       return events;
    }

    private String typeOf(String json){
        try{
            return mapper.readTree(json).get("type").asText();
        }catch (Exception e){
            return "Unknow";
        }
    }
}
