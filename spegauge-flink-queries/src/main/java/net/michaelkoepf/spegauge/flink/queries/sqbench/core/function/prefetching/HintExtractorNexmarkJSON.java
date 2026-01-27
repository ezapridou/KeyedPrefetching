package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;

public abstract class HintExtractorNexmarkJSON<K, T> extends ProcessFunction<NexmarkJSONEvent, T>{
    public final boolean PK;
    public final boolean hintsForAuction;
    private transient ObjectMapper MAPPER;
    private transient JsonFactory FACTORY;

    public long lastHotAuction = 0;

    protected long hotIdsRatio = (200_000 * 3) / 49; // (scenario.numEventsPerSecond * AUCTION_PROPORTION) / PROPORTION_DENOMINATOR;

    SaturatingCMS cms = new SaturatingCMS();

    public abstract boolean isHot(K key);

    public HintExtractorNexmarkJSON(boolean PK, boolean hintsForAuction) {
        this.PK = PK;
        this.hintsForAuction = hintsForAuction;
    }

    @Override
    public void open(OpenContext parameters) {
        MAPPER = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        FACTORY = MAPPER.getFactory();
    }

    public long extractFieldFromJSON(String json, String fieldName) throws Exception {
        try (JsonParser p = FACTORY.createParser(json)) {
            // Move to START_OBJECT so nextFieldName() can see the first field
            if (p.nextToken() != JsonToken.START_OBJECT) {
                throw new IllegalArgumentException("Expected JSON object: " + json);
            }

            String name;
            // Iterate only over FIELD_NAME tokens at the top level
            while ((name = p.nextFieldName()) != null) {
                if (name.equals(fieldName)) {
                    // Move to the value and read as long. getValueAsLong handles numbers or quoted numbers.
                    p.nextToken();
                    return p.getValueAsLong(); // consumes scalar
                } else {
                    // Skip this field's value (object/array/scalar) efficiently
                    p.nextToken();
                    p.skipChildren();
                }
            }
        }
        throw new RuntimeException("Field " + fieldName + " not found in JSON" + json);
    }
}