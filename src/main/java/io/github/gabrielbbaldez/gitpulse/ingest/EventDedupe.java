package io.github.gabrielbbaldez.gitpulse.ingest;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.stereotype.Component;
import java.time.Duration;

@Component
public class EventDedupe {

  private final Cache<String, Boolean> seen = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(10))
            .maximumSize(50_000)
            .build();

  public boolean isNew(String id){
      if(seen.getIfPresent(id) != null){
          return false;
      }
      seen.put(id, true);
      return true;
  }
}
