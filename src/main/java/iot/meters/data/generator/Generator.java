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

    private static final String AVAILABLE = "available";
    private static final String OCCUPIED = "occupied";
    private static final String NOT_AVAILABLE = "NA";
    private static final String OUT_OF_ORDER = "out-of-order";

    @ConfigProperty(name = "interval.milliseconds")
    int intervalMilliSeconds;

    List<JsonObject> meters;

    WeightedCollection<String> stateWeights;

    Random r = new Random();

    @Outgoing("meters")
    public Multi<Message<String>> generate() {
        return Multi.createFrom().ticks().every(Duration.ofMillis(intervalMilliSeconds)).onOverflow().drop()
                .map(tick -> {
                    JsonObject meter = meters.get(r.nextInt(meters.size()));
                    String previous = meter.getString("status");
                    String next = nextStatus(previous);
                    meter.put("status", next);
                    return Message.of(new JsonObject().put("id", meter.getString("id")).put("prev", previous)
                            .put("status", next).put("timestamp", Instant.now().getEpochSecond()).encode());
                });
    }

    private String nextStatus(String status) {
        String next = stateWeights.next();
        if (status.equals(NOT_AVAILABLE) || status.equals(OUT_OF_ORDER) || next.equals(OUT_OF_ORDER)) {
            return next;
        } else if (status.equals(AVAILABLE)) {
            return OCCUPIED;
        } else {
            return AVAILABLE;
        }
    }

    @PostConstruct
    void initMeters() {
        CSV csv = new CSV(',');
        InputStream is = this.getClass().getResourceAsStream("/data/meter_info.csv");
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        meters = reader.lines().skip(1).map(csv::parse).filter(Objects::nonNull)
                .map(f -> new JsonObject().put("id",f.get(0)).put("address",f.get(1))
                        .put("latitude", Double.parseDouble(f.get(2))).put("longitude", Double.parseDouble(f.get(3)))
                        .put("status", NOT_AVAILABLE))
                .collect(Collectors.toList());
        stateWeights = new WeightedCollection<>();
        stateWeights.add(55, OCCUPIED);
        stateWeights.add(50, AVAILABLE);
        stateWeights.add(5, OUT_OF_ORDER);
    }
}
