package net.michaelkoepf.spegauge.generators.sqbench.datageneration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.*;
import net.michaelkoepf.spegauge.api.QueryConfig;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.generators.sqbench.common.HotColdDistributionSamplerAuctionID;
import net.michaelkoepf.spegauge.generators.sqbench.common.HotColdDistributionSamplerPerson;
import net.michaelkoepf.spegauge.generators.sqbench.common.SQBenchConfiguration;
import net.michaelkoepf.spegauge.generators.sqbench.common.SQBenchUtils;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.statistics.distribution.ContinuousDistribution;
import org.apache.commons.statistics.distribution.DiscreteDistribution;
import org.apache.commons.statistics.distribution.UniformDiscreteDistribution;
import org.apache.commons.statistics.distribution.ZipfDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static net.michaelkoepf.spegauge.generators.sqbench.common.HotColdDistributionSamplerPerson.FIRST_PERSON_ID;
import static net.michaelkoepf.spegauge.generators.sqbench.common.HotColdDistributionSamplerPerson.lastBase0PersonId;

/**
 * Generates records of a specific entity.
 */
public final class EntityRecordGenerator {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOGGER =
            LoggerFactory.getLogger(EntityRecordGenerator.class);

    // use a offset to make records same length when they are transferred to the SUT as strings
    private static final long ID_OFFSET = 100_000_000L;
    private static final long LONG_ATTR_OFFSET = 0L;

    private final EntityRecordFull.Type entityType;

    // selectivity attributes (should always be sampled from uniform distribution so selectivity can be easily controlled)
    // E.g., with lower 1 and upper 10, a selectivity of 0.2 can be achieved by comparing the selectivity field
    // to <= 2.
    private static final int SELECTIVITY_LOWER_INCLUSIVE = 0;
    private static final int SELECTIVITY_UPPER_INCLUSIVE = 9;
    private final UniformRandomProvider rng;

    private SQBenchUtils.Entity entity;
    private DiscreteDistribution.Sampler selectivyAttribute1Sampler;
    private DiscreteDistribution.Sampler selectivyAttribute2Sampler;
    private UnionDistributionSampler distributionSamplerWrapperPK;

    // PK
    @Getter
    private long[] PKs;

    // FK
    @Getter
    private EntityRecordFull.Type referencedEntityType;

    @Setter
    private long[] FKs = null; // set when all entities are initialized

    private UnionDistributionSampler distributionSamplerWrapperFK;


    // long attribute 1
    private long[] longAttribute1;

    private UnionDistributionSampler distributionSamplerWrapperLongAttribute1;

    // long attribute 2
    private long[] longAttribute2;

    private UnionDistributionSampler distributionSamplerWrapperLongAttribute2;
    private HotColdDistributionSamplerPerson hotColdDistributionSamplerPerson;
    private HotColdDistributionSamplerAuctionID hotColdDistributionSamplerAuctionID;

    //private DiscreteDistribution.Sampler ysbStringSampler;
    //private YSBHelper ysbHelper;

    // filter attribute
    private UnionDistributionSampler filterAttributeSampler;

    // string attributes
    private int plainStringLength;

    // payload type
    private boolean jsonStringAttribute;
    private boolean xmlStringAttribute;

    private final int numOfQueries;

    public EntityRecordGenerator(UniformRandomProvider rng, SQBenchUtils.Entity entity, int numOfQueries) {
        // these attributes remain unchanged during the lifetime of the generator
        this.rng = rng;
        this.entity = entity;

        this.referencedEntityType = (this.entity.FK != null ? this.entity.FK.references : null);
        this.entityType = entity.entityType;
        this.selectivyAttribute1Sampler = UniformDiscreteDistribution.of(SELECTIVITY_LOWER_INCLUSIVE, SELECTIVITY_UPPER_INCLUSIVE).createSampler(this.rng);
        this.selectivyAttribute2Sampler = UniformDiscreteDistribution.of(SELECTIVITY_LOWER_INCLUSIVE, SELECTIVITY_UPPER_INCLUSIVE).createSampler(this.rng);

        this.numOfQueries = numOfQueries;

        //FileWriterDistr.writeFile();
    }

    public static EntityRecordGenerator copy(EntityRecordGenerator toBeCopied, UniformRandomProvider rng) {
        return new EntityRecordGenerator(rng, toBeCopied.entity, toBeCopied.numOfQueries);
    }

    public void setAttributes(SQBenchUtils.Entity entity, SQBenchConfiguration.Scenario scenario) {
        // check if entityType and referencedEntityType are consistent
        if (this.entityType != entity.entityType) {
            throw new IllegalArgumentException("Entity type mismatch. Entity type cannot change over lifetime of a Generator");
        }

        if (this.referencedEntityType != (entity.FK != null ? entity.FK.references : null)) {
            throw new IllegalArgumentException("Referenced entity type mismatch. Referenced entity type cannot change over lifetime of a Generator");
        }

        // update attributes
        this.entity = entity;

        // PK
        PKs = EntityRecordGeneratorUtils.getConsecutiveLongValues(ID_OFFSET, this.entity.PK.numDistinctValues);
        this.hotColdDistributionSamplerPerson = new HotColdDistributionSamplerPerson(scenario);
        this.hotColdDistributionSamplerAuctionID = new HotColdDistributionSamplerAuctionID(scenario);
        if (this.entity.PK.distribution.type != SQBenchUtils.Distribution.Type.HOT_COLD) {
            this.distributionSamplerWrapperPK = new UnionDistributionSampler(this.entity.PK.distribution.type, this.entity.PK.distribution.parameters, this.rng);
        }
        // FK (values can only be set after all entities have been initialized)
        if (this.referencedEntityType != null && this.entity.FK.distribution.type != SQBenchUtils.Distribution.Type.HOT_COLD) {
            this.distributionSamplerWrapperFK = new UnionDistributionSampler(this.entity.FK.distribution.type, this.entity.FK.distribution.parameters, this.rng);
        }

        // long attributes
        this.longAttribute1 = EntityRecordGeneratorUtils.getConsecutiveLongValues(LONG_ATTR_OFFSET, this.entity.longAttribute1.numDistinctValues);
        this.distributionSamplerWrapperLongAttribute1 = new UnionDistributionSampler(this.entity.longAttribute1.distribution.type, this.entity.longAttribute1.distribution.parameters, this.rng);

        this.longAttribute2 = EntityRecordGeneratorUtils.getConsecutiveLongValues(LONG_ATTR_OFFSET, this.entity.longAttribute2.numDistinctValues);
        this.distributionSamplerWrapperLongAttribute2 = new UnionDistributionSampler(this.entity.longAttribute2.distribution.type, this.entity.longAttribute2.distribution.parameters, this.rng);

        // filter attribute
        if (this.entity.filterAttribute != null) {
            this.filterAttributeSampler = new UnionDistributionSampler(this.entity.filterAttribute.distribution.type, this.entity.filterAttribute.distribution.parameters, this.rng);
        }

        // string attributes
        this.plainStringLength = this.entity.plainStringAttributeLength.length;
        this.jsonStringAttribute = this.entity.jsonStringAttribute.enabled;
        this.xmlStringAttribute = this.entity.xmlStringAttribute.enabled;

        //this.ysbStringSampler = ZipfDistribution.of(YSBHelper.TOTAL_ADS, 1).createSampler(rng);
        //ysbHelper = new YSBHelper();

        if (jsonStringAttribute && xmlStringAttribute) {
            throw new IllegalArgumentException("Only one of JSON and XML string attributes can be enabled");
        }
    }

    public EntityRecordFull nextEntity(long uniqueEventId, long eventTimeStamp) throws JsonProcessingException {
        // PK
        long PK;

        if (this.entityType == EntityRecordFull.Type.A
                && entity.PK.distribution.type == SQBenchUtils.Distribution.Type.HOT_COLD) {
            // auction.id
            PK = HotColdDistributionSamplerAuctionID.lastBase0AuctionId(uniqueEventId)
                    + HotColdDistributionSamplerAuctionID.FIRST_AUCTION_ID;
        }
        else if (this.entityType == EntityRecordFull.Type.B
                && entity.PK.distribution.type == SQBenchUtils.Distribution.Type.HOT_COLD) {
            // bid.bidder
            PK = hotColdDistributionSamplerPerson.sample(uniqueEventId, rng);
        }
        else {
            PK = PKs[distributionSamplerWrapperPK.sample(PKs.length)];
        }

        // FK
        long FK = -1;

        if (FKs != null) {
            if (entityType == EntityRecordFull.Type.B && entity.FK.distribution.type == SQBenchUtils.Distribution.Type.HOT_COLD) {
                // bid.auction
                FK = hotColdDistributionSamplerAuctionID.sample(uniqueEventId, rng);
            }
            else if (this.entityType == EntityRecordFull.Type.C
                    && entity.PK.distribution.type == SQBenchUtils.Distribution.Type.HOT_COLD) {
                // person.id
                FK = HotColdDistributionSamplerPerson.lastBase0PersonId(uniqueEventId) + FIRST_PERSON_ID;
            }
            else{
                FK = FKs[distributionSamplerWrapperFK.sample(FKs.length)];
            }
        }

        // selectivity attributes
        long selectivityAttribute1Sampled = selectivyAttribute1Sampler.sample();
        long selectivityAttribute2Sampled = selectivyAttribute2Sampler.sample();

        // long attributes
        long longAttribute1Sampled = longAttribute1[distributionSamplerWrapperLongAttribute1.sample(longAttribute1.length)];
        long longAttribute2Sampled = longAttribute2[distributionSamplerWrapperLongAttribute2.sample(longAttribute2.length)];

        // filter attribute
        Integer filterAttributeSampled = null;
        if (this.filterAttributeSampler != null) {
            filterAttributeSampled = filterAttributeSampler.sample(0);
        }

        // string attributes
        //String plainString = ysbHelper.getAdIdFast(ysbStringSampler.sample());
        String plainString = EntityRecordGeneratorUtils.generateRandomString(rng, plainStringLength);

        long wallClockTimeStamp = Instant.now().toEpochMilli();

        if (QueryConfig.SELECTED_BENCHMARK == QueryConfig.BENCHMARK_TYPE.YSB){
            if (jsonStringAttribute) {
                if (entityType != EntityRecordFull.Type.A) {
                    throw new UnsupportedOperationException("YSB JSON string generation is only supported for entity type A");
                }
                ObjectNode rootNode = objectMapper.createObjectNode();
                rootNode.put("ad_id_long", PK);
                rootNode.put("pageID", FK);
                rootNode.put("adType", selectivityAttribute1Sampled);
                rootNode.put("eventType", eventTimeStamp);
                rootNode.put("eventTime", selectivityAttribute2Sampled);
                rootNode.put("ip_address", longAttribute2Sampled);
                rootNode.put("adID", plainString);

                return new EntityRecordFull(EntityRecordFull.Type.JSONA, eventTimeStamp, rootNode.toString());
            }
        } else if (QueryConfig.SELECTED_BENCHMARK == QueryConfig.BENCHMARK_TYPE.NEXMARK) {
            if (jsonStringAttribute) {
                if (entityType == EntityRecordFull.Type.A) {
                    ObjectNode rootNode = objectMapper.createObjectNode();
                    rootNode.put("id", PK);
                    rootNode.put("initialBid", FK);
                    rootNode.put("reserve", selectivityAttribute1Sampled);
                    rootNode.put("dateTime", eventTimeStamp);
                    rootNode.put("expires", selectivityAttribute2Sampled);
                    rootNode.put("seller", longAttribute2Sampled);
                    rootNode.put("category", longAttribute1Sampled);
                    rootNode.put("description", plainString);

                    return new EntityRecordFull(EntityRecordFull.Type.JSONA, eventTimeStamp, rootNode.toString(), PK);
                }
                if (entityType == EntityRecordFull.Type.B) {
                    ObjectNode rootNode = objectMapper.createObjectNode();
                    rootNode.put("auction", FK);
                    rootNode.put("bidder", PK);
                    rootNode.put("price", longAttribute1Sampled);
                    rootNode.put("dateTime", eventTimeStamp);
                    rootNode.put("extra", plainString);

                    return new EntityRecordFull(EntityRecordFull.Type.JSONB, eventTimeStamp, PK, FK, rootNode.toString());
                }
                if (entityType == EntityRecordFull.Type.C) {
                    ObjectNode rootNode = objectMapper.createObjectNode();
                    rootNode.put("id", FK);
                    rootNode.put("name", plainString);
                    rootNode.put("creditCard", longAttribute1Sampled);
                    rootNode.put("city", longAttribute2Sampled);
                    rootNode.put("state", selectivityAttribute1Sampled);
                    rootNode.put("dateTime", eventTimeStamp);

                    return new EntityRecordFull(EntityRecordFull.Type.JSONC, eventTimeStamp, FK, rootNode.toString());
                }
            }
        } else if (QueryConfig.SELECTED_BENCHMARK == QueryConfig.BENCHMARK_TYPE.SQBENCH_DEFAULT){
            if (jsonStringAttribute) {
                ObjectNode rootNode = objectMapper.createObjectNode();
                rootNode.put("type", entityType.toString());
                rootNode.put("PK", PK);
                rootNode.put("FK", FK);
                rootNode.put("uniqueTupleId", uniqueEventId);
                rootNode.put("eventTimeStampMilliSecondsSinceEpoch", eventTimeStamp);
                rootNode.put("wallClockTimeStampMilliSecondsSinceEpoch", wallClockTimeStamp);
                rootNode.put("selectivityAttribute1", selectivityAttribute1Sampled);
                rootNode.put("selectivityAttribute2", selectivityAttribute2Sampled);
                rootNode.put("longAttribute1", longAttribute1Sampled);
                rootNode.put("longAttribute2", longAttribute2Sampled);
                rootNode.put("filterAttribute", filterAttributeSampled);
                rootNode.put("plainString", plainString);

                EntityRecordFull.Type type = entityType == EntityRecordFull.Type.A ?
                        EntityRecordFull.Type.JSONA : EntityRecordFull.Type.JSONB;

                return new EntityRecordFull(type, eventTimeStamp, rootNode.toString());
            }
        } else {
            throw new UnsupportedOperationException("Benchmark type not supported: " + QueryConfig.SELECTED_BENCHMARK);
        }

        if (xmlStringAttribute) {
            throw new UnsupportedOperationException("XML string generation not yet implemented");
        }

        return new EntityRecordFull(entityType, uniqueEventId, eventTimeStamp, wallClockTimeStamp, PK, FK,
                selectivityAttribute1Sampled, selectivityAttribute2Sampled, longAttribute1Sampled, longAttribute2Sampled,
                filterAttributeSampled, plainString, null, null);
    }

    public class YSBHelper {
        public static final int TOTAL_ADS = 400_000_000;
        public static final int ADS_PER_CAMPAIGN = 100; // Adjust as needed

        // Generates a deterministic 50-char Ad ID
        public String getAdId(long index) {
            String prefix = String.format("ad_%010d_", index);
            return padRight(prefix, 50, 'a');
        }

        public String getAdIdFast(long index) {
            // 1. Allocate a single char array of the exact final length (50)
            char[] buffer = new char[50];

            // 2. Fill the padding area (indices 14 to 49) with 'a'
            Arrays.fill(buffer, 14, 50, 'a');

            // 3. Set the fixed prefix characters manually
            buffer[0] = 'a';
            buffer[1] = 'd';
            buffer[2] = '_';
            buffer[13] = '_';

            // 4. Write the number backwards (indices 12 down to 3)
            // This handles the "%010d" logic without String parsing
            long val = index;
            for (int i = 12; i >= 3; i--) {
                buffer[i] = (char) ('0' + (val % 10));
                val /= 10;
            }

            // 5. Create the final String once
            return new String(buffer);
        }

        // Generates a deterministic 250-char Campaign ID
        public String getCampaignId(long index) {
            String prefix = String.format("camp_%010d_", index / ADS_PER_CAMPAIGN);
            return padRight(prefix, 250, 'c');
        }

        private String padRight(String s, int n, char filler) {
            StringBuilder sb = new StringBuilder(s);
            while (sb.length() < n) sb.append(filler);
            return sb.toString();
        }
    }

    public static class UnionDistributionSampler {
        private final ContinuousDistribution.Sampler continuousDistributionSampler;
        private final DiscreteDistribution.Sampler discreteDistributionSampler;
        private final SQBenchUtils.Distribution.Type type;

        private final int valueOffset;

        // used if we want the most frequent element of the zipf distribution to be different from 0
        private int numOfElements = 0;
        private int mostFreqElement = 0;

        public UnionDistributionSampler(SQBenchUtils.Distribution.Type type, List<Double> parameters, UniformRandomProvider rng) {
            this.type = type;
            this.continuousDistributionSampler = SQBenchUtils.Distribution.getContinuousDistributionSampler(type, parameters, rng);
            this.discreteDistributionSampler = SQBenchUtils.Distribution.getDiscreteDistributionSampler(type, parameters, rng);

            if (this.continuousDistributionSampler == null && this.discreteDistributionSampler == null) {
                throw new UnsupportedOperationException("Distribution type not supported: " + type);
            }

//            if (this.continuousDistributionSampler != null) {
//                throw new UnsupportedOperationException("Continuous distributions not yet supported");
//            }

            if (this.type == SQBenchUtils.Distribution.Type.ZIPF) {
                if (parameters.size() == 3){
                    this.mostFreqElement = parameters.get(2).intValue();
                    this.numOfElements = parameters.get(0).intValue();

                }
                this.valueOffset = 1;
            } else {
                this.valueOffset = 0;
            }
        }

        public int sample(int n) {
            if (continuousDistributionSampler != null) {
                return (int) Math.round(continuousDistributionSampler.sample() * (n-1));
            } else {
                int frequency = discreteDistributionSampler.sample() - valueOffset;
                int element;
                if (mostFreqElement != 0) {
                    element = frequency + mostFreqElement;
                    if (element >= numOfElements) {
                        element = element - numOfElements;
                    }
                }
                else {
                    element = frequency;
                }
                return element;
            }
        }
    }
}