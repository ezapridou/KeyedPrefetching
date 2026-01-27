package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching;
import java.util.ArrayList;
import java.util.List;

public class CMSTest {
    public static void main(String[] args) {
        // Parameters based on our discussion for 10% frequency detection
        int d = 4;           // Depth
        int w = 10000;       // Width to match 10,000 unique keys
        int b = 8;           // 8-bit saturating counters
        int threshold = 20;  // T threshold
        int delta = 1000;     // Aging interval delta

        SaturatingCMS filter = new SaturatingCMS(d, w, b, threshold, delta);

        // Define our test keys
        String hotKey = "card_1234"; // Should be filtered (hot)
        String coldKey = "card_9999"; // Should NOT be filtered (cold)

        System.out.println("--- Starting CMS Test ---");
        System.out.println("Target: Filter keys covering >10% of traffic.\n");

        // Simulate 1,000 records
        for (int i = 1; i <= 1000; i++) {
            // Simulate skewed distribution:
            // hotKey appears 12% of the time (12 per 100 records)
            if (i % 100 < 12) {
                filter.update(hotKey);
            } else {
                // Other 88% is distributed among unique cold keys
                filter.update("random_key_" + i);
            }

            // Periodically check status to see the "warm up"
            if (i % 200 == 0) {
                System.out.println("After " + i + " records:");
                System.out.println("  Is '" + hotKey + "' hot? " + filter.isHot(hotKey));
                System.out.println("  Is '" + coldKey + "' hot? " + filter.isHot(coldKey));
                System.out.println("-------------------------");
            }
        }

        // Final Verification
        boolean hotKeyFiltered = filter.isHot(hotKey);
        System.out.println("FINAL RESULT:");
        if (hotKeyFiltered) {
            System.out.println("SUCCESS: Hot key was correctly filtered (no hints would be sent).");
        } else {
            System.out.println("FAILURE: Hot key was not detected.");
        }
    }
}
