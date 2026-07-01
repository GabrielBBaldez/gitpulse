<div align="center">

# ⚡ GitPulse

### GitHub's global activity, live — every star is a repository, every pulse is an event.

![Java](https://img.shields.io/badge/Java-21-e76f00?logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5-6db33f?logo=springboot&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231f20?logo=apachekafka)
![Kafka Streams](https://img.shields.io/badge/Kafka%20Streams-windowed-231f20?logo=apachekafka)
![WebSocket](https://img.shields.io/badge/WebSocket-STOMP-46e8ff)

![GitPulse live demo](docs/demo.gif)

**[▶ demo video](GitPulse.mp4)**

GitHub events → Kafka → Kafka Streams → WebSocket → live dashboard.

</div>

---

## Run it

> JDK 21 · Docker · a GitHub PAT (no scopes needed)

```bash
export GITHUB_TOKEN=ghp_xxxxx     # PowerShell: $env:GITHUB_TOKEN="ghp_xxxxx"
docker compose up -d              # Kafka (KRaft, single node)
./mvnw spring-boot:run            # ingestor + streams + websocket + dashboard
# open http://localhost:8080
```

---

<div align="center">

Built by **[Gabriel Baldez](https://github.com/GabrielBBaldez)**

</div>
