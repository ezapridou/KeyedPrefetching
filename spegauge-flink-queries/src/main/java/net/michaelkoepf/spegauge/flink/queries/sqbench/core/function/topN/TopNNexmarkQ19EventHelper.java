package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN;

import net.michaelkoepf.spegauge.api.common.model.sqbench.Bid;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.util.MathUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TopNNexmarkQ19EventHelper implements EventTypeHelper<Long, NexmarkEvent>{
    private static ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
    @Override
    public ValueStateDescriptor<List<NexmarkEvent>> getStateDescriptor(String stateName) {
        return new ValueStateDescriptor<>(stateName, Types.LIST(Types.POJO(NexmarkEvent.class)));
    }

    @Override
    public boolean compareElements(NexmarkEvent elem1, NexmarkEvent elem2) {
        return elem1.bid.price > elem2.bid.price;
    }

    @Override
    public void fillStaticTable(KeyGroupRange keyGroupRange, int maxParallelism,
                                          KeyAccessibleState<Long, List<NexmarkEvent>> state, int bidsPerAuction){

        int numAuctions = 60_000_000;

        int length  = 152;
        final byte[] buffer = new byte[length];
        int firstAuctionId = 1000;

        for (long auctionId = firstAuctionId; auctionId < numAuctions + firstAuctionId; auctionId++) {
            /*if (auctionId % 4_000_000 == 0) {
                System.out.println("Now at " + auctionId);
            }*/
            if (!keyGroupRange.contains(getKeyGroupIndex(auctionId, maxParallelism))){
                continue;
            }
            List<NexmarkEvent> bids = new ArrayList<>(bidsPerAuction);
            for (int b = 0; b < bidsPerAuction; b++) {
                NexmarkEvent event = getRandomNexmarkEvent(auctionId, buffer);
                bids.add(event);
            }

            try {
                state.update(auctionId, bids);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        //System.out.println("Finished filling static table");
    }

    public NexmarkEvent getRandomNexmarkEvent(long auctionId, byte[] buffer){
        // generate random price between 0 and 99999999
        //long price = (prevPrice <= 10000000) ? prevPrice : threadLocalRandom.nextLong(10000000, prevPrice);
        long price = threadLocalRandom.nextLong(0, 100000000);

        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = (byte) threadLocalRandom.nextInt(256);
        }
        String payload = new String(buffer, StandardCharsets.ISO_8859_1); // 1:1 bytes->chars

        long bidder = threadLocalRandom.nextLong(1, 100_000_000);
        long time = threadLocalRandom.nextLong(1, 100_000_000);
        long processTime = threadLocalRandom.nextLong(1, 100_000_000);
        Bid bid = new Bid(auctionId, bidder, price, time,payload);
        return new NexmarkEvent(bid, time, processTime);
    }

    public int getKeyGroupIndex(Long key, int maxParallelism) {
        return MathUtils.murmurHash((key).hashCode()) % maxParallelism;
    }
}
