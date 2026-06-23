# GitPulse — live GitHub activity stream

> Ingests the public GitHub events firehose, turns a polling REST endpoint into a
> real-time stream through Kafka, processes it with Kafka Streams, and pushes live
> activity + trending repositories to a browser dashboard over WebSocket.

*(working name — rename freely)*

---

## 0. How to use this document

**This spec is self-contained. It is the single source of truth for building GitPulse from
scratch.** A developer (or an AI assistant in a fresh conversation with zero prior context)
should be able to build the entire project by reading this file top-to-bottom and
implementing the phases in order (§15).

- Every technical decision is already made (§4) — don't re-litigate the stack, just build it.
- Build **incrementally, one phase at a time** (§15). Each phase compiles, runs, and is demoable on its own.
- Phase 0 is the hard part (taming the GitHub API). Do it first and get it solid before adding Kafka.
- There is **no CDC and no database** in this project — it is a pure stream-processing pipeline.

---

## 1. What it is

A backend that taps GitHub's global public-activity feed (pushes, PRs, issues, stars,
forks, releases…), runs it through Kafka, computes live aggregations (events/min,
trending repos, top languages), and streams the result to a web dashboard that updates in
real time. A personal **watchlist** lets you pin the repos you care about and see their
activity as it happens.

## 2. What it demonstrates (portfolio goals)

- **Event-driven core** — Kafka + Kafka Streams doing real windowed aggregation.
- **Poll → stream adapter** — GitHub's firehose is a *polling REST endpoint*, not a socket.
  Turning it into a reliable, deduplicated, rate-limit-aware stream is the headline skill here.
- **Async / reactive ingestion** with Spring `WebClient`.
- **Live push to the browser** over WebSocket (STOMP).
- Cloud story for the README: AWS **MSK** (managed Kafka) + **ECS/Fargate**.

---

## 3. Architecture

```
                    poll (ETag / 304 / X-Poll-Interval)
GitHub Events API  ───────────────►  Ingestor (Spring WebClient)
 GET /events                              │ dedup by event id
                                          │ publish
                                          ▼
                                    Kafka  ──topic: github.events.raw  (key = repo name)
                                          │
                                          ▼
                                  Kafka Streams
                          (windowed counts · trending repos ·
                           top languages · spike alerts)
                                          │
                                          ▼
                                  Broadcaster (Kafka consumer)
                                          │  STOMP
                                          ▼
                                  WebSocket  ──►  Web dashboard (SPA)
                          /topic/feed · /topic/stats · /topic/trending · /topic/alerts
```

Optional **replay mode** (Phase 5): instead of the live API, feed historical hours from
[GH Archive](https://www.gharchive.org) into `github.events.raw` at accelerated speed — for
high-throughput demos that don't depend on the live rate limit.

---

## 4. Tech stack & decisions (already decided — just build it)

| Concern | Decision |
|--------|----------|
| Language / framework | **Java 21**, **Spring Boot 3.3.x** |
| Build | **Maven** (with `./mvnw` wrapper) |
| App shape | **Single modular monolith** — packages, not multi-module. One `mvn spring-boot:run`. |
| GitHub ingestion | **Spring `WebClient`** (need raw header/ETag/status control — do **not** use a GitHub SDK) |
| Messaging | **Apache Kafka**, **KRaft** mode (no ZooKeeper), single node via Docker Compose |
| Stream processing | **Kafka Streams** (`@EnableKafkaStreams`) |
| Serialization | JSON (Jackson) — custom `JsonSerde<GithubEvent>` |
| Dedup | **Caffeine** cache of seen event ids (TTL ~10 min) |
| Push to browser | **Spring WebSocket + STOMP**, SockJS fallback |
| Frontend | **Plain HTML + vanilla JS** served as Spring static resources (no Node/bundler). SockJS + StompJS + Chart.js via CDN. |
| Base package | `com.gitpulse` (rename to your own, e.g. `io.github.<you>.gitpulse`) |

> Rationale for the front choice: zero front-end toolchain keeps the project a single
> `mvn spring-boot:run` to run end-to-end. Swap in React later if desired — the WebSocket
> contracts (§12) don't change.

---

## 5. Data source — the GitHub Events API

**It is a REST endpoint you poll, not a stream.** This is the whole point of the ingestor.

### Primary endpoint

```
GET https://api.github.com/events
Accept: application/vnd.github+json
X-GitHub-Api-Version: 2022-11-28
Authorization: Bearer <YOUR_PAT>      # optional but strongly recommended
User-Agent: gitpulse                  # GitHub requires a User-Agent
```

Returns a JSON array of public event objects, most recent first. Capped at ~**300 events**
total (≈30 per page, up to 10 pages via the `Link` header). For the live feed, **page 1 is
enough**; walk `Link` only for backfill.

### Auth & rate limits

| Mode | Limit |
|------|-------|
| Unauthenticated (per IP) | **60 requests/hour** — unusable |
| Authenticated with a PAT | **5,000 requests/hour** |

A classic or fine-grained **Personal Access Token** with **no special scopes** is enough
for public events. Provide it via the `GITHUB_TOKEN` env var.

### The polling protocol (implement this exactly)

1. **Respect `X-Poll-Interval`** — the response header gives the minimum seconds before the
   next poll (this endpoint is cached, typically ~60s). Schedule the next poll for
   `max(X-Poll-Interval, configured-min)`.
2. **Conditional requests with `ETag`** — store the `ETag` from each 200 response and send
   it back as `If-None-Match` on the next request. If nothing changed you get
   **`304 Not Modified`** with an empty body — and **304s do not count against the primary
   rate limit.** Free polling.
3. **Deduplicate by event `id`** — consecutive polls overlap; the same event appears more
   than once. Keep a Caffeine cache of seen ids and drop repeats before publishing.
4. **Watch `X-RateLimit-Remaining` / `X-RateLimit-Reset`** and back off as it nears zero.

### Event object shape

```json
{
  "id": "48572934109",
  "type": "PullRequestEvent",
  "actor": { "login": "octocat", "avatar_url": "https://..." },
  "repo":  { "name": "apache/iceberg", "url": "https://api.github.com/repos/apache/iceberg" },
  "payload": { "action": "opened", "number": 16794, "...": "..." },
  "public": true,
  "created_at": "2026-06-22T15:04:21Z"
}
```

`payload` varies by `type`. Event types you'll use:

| Type | Meaning |
|------|---------|
| `PushEvent` | Commits pushed |
| `PullRequestEvent` | PR opened / closed / merged (`payload.action`) |
| `IssuesEvent` / `IssueCommentEvent` | Issue activity |
| `WatchEvent` | **A repo was starred** ("watch" = star) |
| `ForkEvent` | Repo forked |
| `CreateEvent` / `DeleteEvent` | Branch/tag/repo created or deleted |
| `ReleaseEvent` | A release was published |
| `PullRequestReviewEvent` | A PR review |

### Related endpoints (for the watchlist — Phase 4)

| Endpoint | Use |
|----------|-----|
| `GET /repos/{owner}/{repo}/events` | Events for one specific repo |
| `GET /networks/{owner}/{repo}/events` | A repo **and all its forks** |
| `GET /users/{username}/events[/public]` | A user's activity |
| `GET /orgs/{org}/events` | An org's activity |

### Enrichment (optional — mind the rate limit)

`GET /repos/{owner}/{repo}` → `language`, `stargazers_count`, `description`. Cache
aggressively; each call burns rate limit.

### Honest caveats

- The public `/events` feed is **cached (~60s)** and is a *sample* of recent public
  activity (~300 events), not 100% of GitHub's volume. Plenty for a live dashboard; not a
  true high-frequency firehose.
- **Smoothing trick** (frontend or broadcaster): each poll returns a *batch*. Instead of
  dumping 30 events at once, buffer them and "unspool" spaced over the poll interval so the
  UI looks like a continuous stream.
- For genuinely high throughput, use **GH Archive replay** (Phase 5).

---

## 6. Project structure

```
gitpulse/
├── pom.xml
├── docker-compose.yml
├── README.md                       (this file)
├── mvnw / mvnw.cmd / .mvn/
└── src/
    ├── main/
    │   ├── java/com/gitpulse/
    │   │   ├── GitpulseApplication.java
    │   │   ├── ingest/
    │   │   │   ├── GitHubClient.java          # WebClient wrapper, ETag + headers
    │   │   │   ├── GitHubEventPoller.java     # self-rescheduling poll loop
    │   │   │   └── EventDedupe.java           # Caffeine seen-id cache
    │   │   ├── model/
    │   │   │   └── GithubEvent.java           # record (parsed event)
    │   │   ├── kafka/
    │   │   │   ├── Topics.java                # topic name constants
    │   │   │   ├── KafkaConfig.java           # producer + JsonSerde beans
    │   │   │   └── EventProducer.java         # publish to github.events.raw
    │   │   ├── stream/
    │   │   │   ├── StreamsConfig.java         # @EnableKafkaStreams
    │   │   │   ├── EventTopology.java         # the Kafka Streams topology
    │   │   │   └── dto/                       # StatsSnapshot, TrendingEntry, Alert
    │   │   ├── web/
    │   │   │   ├── WebSocketConfig.java       # STOMP endpoints + broker
    │   │   │   └── Broadcaster.java           # Kafka consumer → SimpMessagingTemplate
    │   │   └── watchlist/
    │   │       └── WatchlistService.java      # Phase 4
    │   └── resources/
    │       ├── application.yml
    │       └── static/
    │           ├── index.html
    │           ├── app.js
    │           └── styles.css
    └── test/java/com/gitpulse/...
```

---

## 7. Maven dependencies (`pom.xml`)

Spring Boot parent `3.3.x`, Java 21. Starters / libraries:

- `spring-boot-starter-web`
- `spring-boot-starter-webflux`            (for `WebClient`)
- `spring-boot-starter-websocket`
- `spring-kafka`
- `org.apache.kafka:kafka-streams`
- `com.github.ben-manes.caffeine:caffeine`
- `org.projectlombok:lombok`               (optional)
- Test: `spring-boot-starter-test`, `spring-kafka-test`

(Jackson comes transitively with `-web`.)

---

## 8. Configuration (`src/main/resources/application.yml`)

```yaml
server:
  port: 8080

github:
  api-base-url: https://api.github.com
  token: ${GITHUB_TOKEN:}            # PAT; empty = unauthenticated (60/h)
  user-agent: gitpulse
  poll:
    min-interval-seconds: 60         # never poll faster than this or X-Poll-Interval
  dedupe:
    ttl-minutes: 10
    max-size: 50000

spring:
  kafka:
    bootstrap-servers: localhost:9092
    streams:
      application-id: gitpulse-streams
      properties:
        commit.interval.ms: 1000
        num.stream.threads: 1
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: gitpulse-broadcaster
      auto-offset-reset: latest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.gitpulse.*"

# Phase 4
watchlist:
  repos:
    - apache/seatunnel
    - apache/iceberg
    - delta-io/delta

# Phase 5 (replay mode — off by default)
replay:
  enabled: false
  archive-url: https://data.gharchive.org
```

---

## 9. Local infra (`docker-compose.yml`)

Single-node Kafka in KRaft mode. The official `apache/kafka` image boots KRaft with sane
defaults and advertises `localhost:9092` for host apps:

```yaml
services:
  kafka:
    image: apache/kafka:3.8.0
    container_name: gitpulse-kafka
    ports:
      - "9092:9092"
```

`docker compose up -d` and you have a broker on `localhost:9092`. (If you prefer explicit
listener config or a UI, `bitnami/kafka` or adding `provectuslabs/kafka-ui` works too.)
Topics auto-create by default; otherwise create them with the `kafka-topics.sh` tool inside
the container.

---

## 10. Domain model

```java
// com.gitpulse.model.GithubEvent
public record GithubEvent(
    String id,
    String type,            // PushEvent, PullRequestEvent, WatchEvent, ...
    String actorLogin,
    String actorAvatarUrl,
    String repoName,        // "owner/repo"
    Instant createdAt,
    JsonNode payload        // raw payload, type-specific
) {}
```

Parse it from the API JSON in `GitHubClient` (flatten `actor.login`, `repo.name`, etc.).

---

## 11. Kafka topics & message contracts

| Topic | Key | Value | Produced by |
|-------|-----|-------|-------------|
| `github.events.raw` | `repoName` | `GithubEvent` (JSON) | Ingestor (`EventProducer`) |

Keying by `repoName` keeps all events for a repo on one partition (preserves per-repo
ordering for trending). Derived stats are computed inside the Streams topology and pushed
straight to WebSocket by the broadcaster — they don't need their own Kafka topics for v1
(keep it simple; add output topics only if you split services later).

Define names as constants:

```java
// com.gitpulse.kafka.Topics
public final class Topics {
    public static final String EVENTS_RAW = "github.events.raw";
}
```

---

## 12. WebSocket / STOMP

- SockJS endpoint: `/ws`
- Broker prefix: `/topic`

| Destination | Payload | Cadence |
|-------------|---------|---------|
| `/topic/feed` | one event | per event (smoothed) |
| `/topic/stats` | counts-by-type snapshot | per window close (~1/min) |
| `/topic/trending` | top-N repos | every few seconds |
| `/topic/alerts` | spike alert | on trigger |

Payload contracts:

```jsonc
// /topic/feed
{ "id": "...", "type": "WatchEvent", "actor": "octocat",
  "avatar": "https://...", "repo": "apache/iceberg", "at": "2026-06-22T15:04:21Z" }

// /topic/stats
{ "windowEnd": "2026-06-22T15:05:00Z",
  "counts": { "PushEvent": 812, "PullRequestEvent": 143, "WatchEvent": 207, "...": 0 } }

// /topic/trending
{ "windowSeconds": 300,
  "top": [ { "repo": "apache/iceberg", "score": 42, "stars": 30, "forks": 12 }, ... ] }

// /topic/alerts
{ "repo": "some/repo", "kind": "STAR_SPIKE", "count": 25, "windowSeconds": 60,
  "at": "2026-06-22T15:04:50Z" }
```

---

## 13. Stream processing (Kafka Streams topology)

Source: `github.events.raw` with `Consumed.with(Serdes.String(), jsonEventSerde)`.

**A. Counts by type (→ `/topic/stats`)**
- `groupBy((repo, e) -> e.type())`
- `windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))`
- `.count()`
- On window emit, assemble a `StatsSnapshot` and forward to the broadcaster (via an output
  topic or a `KStream.foreach` that calls `SimpMessagingTemplate`).

**B. Trending repos (→ `/topic/trending`)**
- `filter` to `WatchEvent`, `ForkEvent`, `PullRequestEvent`
- `groupBy((repo, e) -> e.repoName())`
- hopping window: `TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30)).advanceBy(Duration.ofSeconds(30))`
- `.count()` per repo per window
- **Top-N is awkward in pure Streams** — pragmatic approach: forward per-repo `(repo, count)`
  updates to the broadcaster, which keeps an in-memory bounded top-N (e.g. a `TreeMap`/
  `PriorityQueue` capped at N) and pushes the current leaderboard to `/topic/trending` every
  ~3s. Score = weighted sum (e.g. `stars*2 + forks*1 + prs*1`).

**C. Spike alerts (→ `/topic/alerts`)**
- `filter` to `WatchEvent`
- `groupBy(repoName)` → 1-min tumbling window → `.count()`
- `.filter((repo, count) -> count >= threshold)` → emit `Alert`

Use one `JsonSerde<GithubEvent>` bean for the source; built-in serdes for `Long`/`String`
aggregates.

---

## 14. Frontend (`src/main/resources/static/`)

Single page, no build step. Load via CDN in `index.html`:

- `sockjs-client`, `@stomp/stompjs`, `chart.js`.

`app.js` responsibilities:
1. Connect STOMP over SockJS to `/ws`.
2. Subscribe to `/topic/feed`, `/topic/stats`, `/topic/trending`, `/topic/alerts`.
3. Render:
   - **Feed** — prepend events into a scrolling list (cap ~100 rows; fade-in animation).
   - **Stats** — a Chart.js bar/line chart updated from `/topic/stats`.
   - **Trending** — a table that re-sorts and flashes rows that moved.
   - **Alerts** — toast/banner on `/topic/alerts`.
   - **Throughput counter** — events/sec computed client-side from feed arrivals.

Keep the look clean and dark; the "wow" is motion (rows appearing, leaderboard reordering).

---

## 15. Build phases (do them in order)

Each phase compiles, runs, and is demoable. Don't skip ahead.

### Phase 0 — The poller (no Kafka yet)
**Goal:** reliably pull events from GitHub and log them.
**Build:** `GitHubClient` (WebClient: base URL, auth header, User-Agent; method that does a
conditional GET and returns status + headers + body — use `exchangeToMono` so you can read a
304 with no body), `EventDedupe` (Caffeine), `GitHubEventPoller` (a self-rescheduling loop
on a `ScheduledExecutorService`: poll → store ETag → parse → dedupe → log; schedule next run
at `max(X-Poll-Interval, min-interval)`).
**Key logic:** send `If-None-Match` with the stored ETag; on 304, do nothing; on 200, update
ETag and process. Drop events already in the dedupe cache.
**Done when:** running the app prints a steady stream of fresh, non-duplicated events, and
304s are visible in logs (no rate-limit burn).

### Phase 1 — Kafka + WebSocket end to end
**Goal:** event → Kafka → browser.
**Build:** `KafkaConfig` (`JsonSerde`/producer), `EventProducer` (publish `GithubEvent` to
`github.events.raw`, key = repoName); poller now publishes instead of logging.
`WebSocketConfig` (STOMP `/ws` + `/topic` broker), `Broadcaster` (`@KafkaListener` on
`github.events.raw` → map to feed DTO → `SimpMessagingTemplate.convertAndSend("/topic/feed")`).
Minimal `index.html` + `app.js` that connects and appends to a list.
**Done when:** events appear live in the browser feed.

### Phase 2 — Windowed counts
**Goal:** live events/min by type.
**Build:** `StreamsConfig` (`@EnableKafkaStreams`), `EventTopology` part **A**. Broadcaster
pushes `StatsSnapshot` to `/topic/stats`; front renders a Chart.js chart + throughput counter.
**Done when:** the chart updates each minute and the events/sec counter ticks.

### Phase 3 — Trending repos
**Goal:** live trending leaderboard.
**Build:** topology part **B**; broadcaster maintains in-memory top-N and pushes
`/topic/trending`; front renders a table that re-sorts and flashes on change.
**Done when:** the leaderboard reorders live as repos gain stars/forks.

### Phase 4 — Watchlist + spike alerts
**Goal:** personal relevance + alerting.
**Build:** `WatchlistService` (repos from `application.yml`); a filtered feed view
(`/topic/feed` consumers can filter client-side, or add `/topic/watchlist`). Optionally poll
`GET /repos/{owner}/{repo}/events` for pinned repos to guarantee coverage. Topology part
**C** → `/topic/alerts`; front shows toasts.
**Done when:** you can pin repos and get their events + spike alerts.

### Phase 5 — GH Archive replay mode
**Goal:** high-throughput demo without the live rate limit.
**Build:** a `ReplaySource` (enabled by `replay.enabled`) that downloads an hour file from
`https://data.gharchive.org/YYYY-MM-DD-H.json.gz`, decompresses, and publishes events to
`github.events.raw` at an accelerated, configurable rate. Same downstream pipeline.
**Done when:** flipping `replay.enabled=true` floods the dashboard from history at speed.

### Phase 6 — Polish
Docker Compose for the whole stack, a clean dashboard, error handling/backoff on the poller,
a short architecture section + screenshot/GIF in this README, and AWS deployment notes
(MSK + ECS/Fargate). Optional: a `kafka-ui` container, basic tests (poller dedup, topology
with `TopologyTestDriver`).

---

## 16. Running locally

**Prerequisites:** JDK 21, Docker + Docker Compose, a GitHub **Personal Access Token**.

```bash
export GITHUB_TOKEN=ghp_xxxxx        # your PAT (Windows PowerShell: $env:GITHUB_TOKEN="ghp_xxxxx")
docker compose up -d                 # Kafka (KRaft) on localhost:9092
./mvnw spring-boot:run               # ingestor + streams + websocket gateway
# open http://localhost:8080
```

---

## 17. Engineering notes / gotchas

- **304 handling** — read it with `exchangeToMono`; `retrieve()` will try to deserialize an
  empty body. A 304 means "nothing new", not an error.
- **User-Agent is mandatory** — GitHub rejects requests without one.
- **Dedup is essential** — overlapping polls re-deliver events; without dedup the feed
  double-counts everything.
- **Partition by repo** — preserves per-repo ordering for trending/alerts.
- **Top-N lives in the broadcaster**, not in Streams — pure-Streams global top-N is painful
  and not worth it here.
- **Window grace + emit timing** — counts emit on window close; use a small grace and
  `commit.interval.ms` ~1s so the UI feels live.
- **Replay vs live** are mutually exclusive sources into the same topic — guard with
  `replay.enabled`.

## 18. Stretch ideas

- Language breakdown (enrich via `GET /repos/{owner}/{repo}`, cached).
- Per-event-type filters in the UI.
- Persist trending history to show "trending over the last hour".
- Deploy to AWS (MSK + Fargate) and put a live URL in the README.

## 19. Interview talking points

- Turning a **polling REST endpoint into a reliable stream** (conditional requests, 304s
  that don't cost rate limit, dedup, backoff, honoring `X-Poll-Interval`).
- **Kafka Streams windowing** (tumbling vs hopping) and why top-N belongs in the consumer.
- **Partitioning strategy** and per-repo ordering.
- **At-least-once vs exactly-once** processing in Kafka Streams.
- Replaying history from **GH Archive** to load-test the pipeline without the live limit.
```
