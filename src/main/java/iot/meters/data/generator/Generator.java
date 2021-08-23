package iot.meters.data.generator;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Multi;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class Generator {

    static final Logger log = LoggerFactory.getLogger(Generator.class);

    @ConfigProperty(name = "interval.milliseconds")
    int intervalMilliSeconds;

    List<JsonObject> meters;

    WeightedCollection<String> stateWeights;

    Random r = new Random();

    @Outgoing("meters")
    public Multi<Message<String>> generate() {
        return Multi.createFrom().ticks().every(Duration.ofMillis(intervalMilliSeconds)).onOverflow().drop()
                .map(tick -> Message.of(meters.get(r.nextInt(meters.size())).copy()
                        .put("timestamp", Instant.now().getEpochSecond()).put("status", stateWeights.next()).encode()));
    }

    @PostConstruct
    void initMeters() {
        CSV csv = new CSV(',');
        InputStream is = this.getClass().getResourceAsStream("/data/meter_info.csv");
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        meters = reader.lines().skip(1).map(csv::parse).filter(Objects::nonNull)
                .map(f -> new JsonObject().put("uuid",f.get(0)).put("address",f.get(1))
                        .put("latitude", Double.parseDouble(f.get(2))).put("longitude", Double.parseDouble(f.get(3))))
                .collect(Collectors.toList());
        stateWeights = new WeightedCollection<>();
        stateWeights.add(55, "occupied");
        stateWeights.add(25, "available");
        stateWeights.add(10, "unknown");
        stateWeights.add(10, "out-of-service");
    }
}
