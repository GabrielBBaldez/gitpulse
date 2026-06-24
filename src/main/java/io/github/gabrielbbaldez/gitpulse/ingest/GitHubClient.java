package io.github.gabrielbbaldez.gitpulse.ingest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
public class GitHubClient {
    private final WebClient webClient;
    private String etag;

    public GitHubClient(@Value("${github.token:}") String token) {
        WebClient.Builder builder = WebClient.builder()
                .baseUrl("https://api.github.com")
                .defaultHeader("User-Agent", "gitpulse");
        if (token != null && !token.isBlank()) {
            builder.defaultHeader("Authorization", "Bearer " + token);
        }
        this.webClient = builder.build();
    }

    public String fetchEvents(){
       return webClient.get()
               .uri("/events")
               .headers(h -> {
                   if(etag != null){
                       h.setIfNoneMatch(etag);
                   }
               })
               .exchangeToMono( response -> {
                   if (response.statusCode().value() == 304){
                       return Mono.empty();
                   }
                   this.etag = response.headers().asHttpHeaders().getETag();
                   System.out.println("rate limit: " + response.headers().asHttpHeaders().getFirst("X-RateLimit-Limit"));
                   return response.bodyToMono(String.class);
               })
               .block();
    }
}
