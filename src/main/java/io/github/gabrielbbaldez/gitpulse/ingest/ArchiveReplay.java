package io.github.gabrielbbaldez.gitpulse.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.gabrielbbaldez.gitpulse.kafka.EventProducer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.zip.GZIPInputStream;

@Component
public class ArchiveReplay  implements CommandLineRunner {

    private final EventProducer eventProducer;
    private final ObjectMapper objectMapper;

    public ArchiveReplay(EventProducer eventProducer, ObjectMapper objectMapper) {
        this.eventProducer = eventProducer;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(String... args) throws Exception {
        for (int back = 4; back <= 24; back++) {
            String stamp = ZonedDateTime.now(ZoneOffset.UTC).minusHours(back)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd-H"));
            String url = "https://data.gharchive.org/" + stamp + ".json.gz";
            try (var in = new GZIPInputStream(URI.create(url).toURL().openStream());
                 var reader = new BufferedReader(new InputStreamReader(in))) {
                System.out.println("using archive " + stamp);
                String line;
                int count = 0;
                while ((line = reader.readLine()) != null) {
                    String repo = objectMapper.readTree(line).get("repo").get("name").asText();
                    eventProducer.publish(repo,line);
                    count++;
                    if (count % 20 == 0) Thread.sleep(30);
                }
                System.out.println("read " + count + " events from archive");
                return;
            } catch (FileNotFoundException e) {
                System.out.println("archive not ready for " + stamp + ", trying earlier...");
            }
        }
    }
}

