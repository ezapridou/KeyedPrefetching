package net.michaelkoepf.spegauge.flink.queries.ysb;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import java.util.UUID;

public class RedisPopulator {

    public static final int TOTAL_ADS = 400_000_000;
    public static final int ADS_PER_CAMPAIGN = 100; // Adjust as needed

    // Generates a deterministic 50-char Ad ID
    public static String getAdId(long index) {
        //String prefix = String.format("ad_%010d_", index);
        return Long.toString(index);//padRight(prefix, 50, 'a');
    }

    // Generates a deterministic 250-char Campaign ID
    public static String getCampaignId(long index) {
        String prefix = String.format("camp_%010d_", index / ADS_PER_CAMPAIGN);
        return padRight(prefix, 250, 'c');
    }

    private static String padRight(String s, int n, char filler) {
        StringBuilder sb = new StringBuilder(s);
        while (sb.length() < n) sb.append(filler);
        return sb.toString();
    }
    public static void main(String[] args) {
        // 1. Connect to your local Docker Redis
        try (Jedis jedis = new Jedis("10.90.46.32", 6379, 60000)) {
            System.out.println("Connected to Redis. Flushing existing data...");
            jedis.flushDB();

            System.out.println("Starting population of 400M keys...");
            Pipeline p = jedis.pipelined();

            for (int i = 0; i < TOTAL_ADS; i++) {
                p.set(getAdId(i), getCampaignId(i));

                // Sync every 10,000 records to manage memory and network
                if (i % 10000 == 0) {
                    p.sync();
                    if (i % 1000000 == 0) System.out.println("Inserted " + i + " records...");
                }
            }
            p.sync();
            System.out.println("Population complete.");
        }

        /*try (Jedis jedis = new Jedis("10.90.46.32", 6379)) {
            // Configuration for "Large State"
            int numCampaigns = 100;
            int adsPerCampaign = 10; // Increase this to scale state size
            int batchSize = 5;//5000;

            System.out.println("Starting population of " + (numCampaigns * adsPerCampaign) + " ads...");
            long startTime = System.currentTimeMillis();

            // 2. Use Pipelining for high performance
            Pipeline p = jedis.pipelined();
            int count = 0;

            for (int i = 0; i < numCampaigns; i++) {
                String campaignId = UUID.randomUUID().toString();
                if (i==0) {
                    campaignId = "Camp";
                }

                for (int j = 0; j < adsPerCampaign; j++) {
                    String adId = UUID.randomUUID().toString();

                    if (i==0 && j==0) {
                        adId = "Ad";
                    }

                    p.set(adId, campaignId);
                    count++;

                    // Sync every batch to avoid OOM on the client
                    if (count % batchSize == 0) {
                        p.sync();
                    }
                }
            }
            p.sync(); // Final sync for remaining items

            long endTime = System.currentTimeMillis();
            System.out.println("Success! Populated " + count + " keys in " + (endTime - startTime) + "ms");
            System.out.println(jedis.get("Ad"));
        }*/
    }
}