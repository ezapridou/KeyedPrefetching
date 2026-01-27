package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN;

import net.michaelkoepf.spegauge.api.common.model.sqbench.Bid;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.util.MathUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TopNNexmarkQ18EventHelper implements EventTypeHelper<Tuple2<Long, Long>, NexmarkEvent>{
    private static ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
    @Override
    public ValueStateDescriptor<List<NexmarkEvent>> getStateDescriptor(String stateName) {
        return new ValueStateDescriptor<>(stateName, Types.LIST(Types.POJO(NexmarkEvent.class)));
    }

    @Override
    public boolean compareElements(NexmarkEvent elem1, NexmarkEvent elem2) {
        return elem1.bid.dateTime > elem2.bid.dateTime;
    }

    @Override
    public void fillStaticTable(KeyGroupRange keyGroupRange, int maxParallelism,
                                KeyAccessibleState<Tuple2<Long, Long>, List<NexmarkEvent>> state, int bidsPerAuction){
        // (auction, bid) --> Bid ---- 216 bytes
        // 950M entries --> ~205 GB
        // 150000(unique auctions)*(150000/23 unique bidders)*216 --> 211gb
        int numAuctions = 150_000;
        int numBidders = 150_000/23;

        int firstAuctionId = 1000;
        int firstPersonId = 1000;

        int length  = 152;
        final byte[] buffer = new byte[length];

        for (long auctionId = firstAuctionId; auctionId < numAuctions + firstAuctionId; auctionId++) {
            if (auctionId % 4_000_000 == 0) {
                System.out.println("Now at " + auctionId);
            }
            for (long bidderId = firstPersonId; bidderId < numBidders + firstPersonId; bidderId++) {
                Tuple2<Long, Long> key = Tuple2.of(bidderId, auctionId);
                if (!keyGroupRange.contains(getKeyGroupIndex(key, maxParallelism))){
                    continue;
                }
                List<NexmarkEvent> bids = new ArrayList<>(bidsPerAuction);
                NexmarkEvent bid = getRandomNexmarkEvent(auctionId, bidderId, buffer);
                bids.add(bid);
                try {
                    state.update(key, bids);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }

    public NexmarkEvent getRandomNexmarkEvent(long auctionId, long bidder, byte[] buffer){
        long price = threadLocalRandom.nextLong(0, 100000000);

        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = (byte) threadLocalRandom.nextInt(256);
        }
        String payload = new String(buffer, StandardCharsets.ISO_8859_1); // 1:1 bytes->chars

        long time = threadLocalRandom.nextLong(1, 100_000_000);
        long processTime = threadLocalRandom.nextLong(1, 100_000_000);
        Bid bid = new Bid(auctionId, bidder, price, time,payload);
        return new NexmarkEvent(bid, time, processTime);
    }

    public int getKeyGroupIndex(Tuple2<Long, Long> key, int maxParallelism) {
        return MathUtils.murmurHash((key).hashCode()) % maxParallelism;
    }
}

