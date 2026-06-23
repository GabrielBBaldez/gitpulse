package io.github.gabrielbbaldez.gitpulse.ingest;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class StartupRunner implements CommandLineRunner {
    private final GitHubClient gitHubClient;

    public StartupRunner(GitHubClient gitHubClient) {
        this.gitHubClient = gitHubClient;
    }


    @Override
    public void run(String... args) throws Exception {
        System.out.println(gitHubClient.fetchEvents());
    }
}
