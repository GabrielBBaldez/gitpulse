package io.github.gabrielbbaldez.gitpulse.ingest;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Component
public class GitHubClient {
    private final WebClient webClient;

    public GitHubClient() {
        this.webClient = WebClient.
                builder().
                baseUrl("https://api.github.com").
                defaultHeader("User-Agent","gitpulse").
                build();
    }

    public String fetchEvents(){
       String result = webClient.get().uri("/events").retrieve().bodyToMono(String.class).block();
       return result;
    }
}
