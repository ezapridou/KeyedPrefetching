package net.michaelkoepf.spegauge.api.sut;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.HashMap;
public class JoinDataDistrMergeStats implements Serializable {
    // key: data range pair, value: number of matches, total number of elements
    public HashMap<AbstractMap.SimpleImmutableEntry<DataRange, DataRange>,
            Long> joinMatchesPerDataRangePair;
    public HashMap<DataRange, Long> totalNumElementsPerDataRange;

    public final int targetSampleSizeA = 8500;
    public final int targetSampleSizeB = 1500;

    public int actualSampleSizeA = 0;
    public int actualSampleSizeB = 0;

    public JoinDataDistrMergeStats() {
        this.joinMatchesPerDataRangePair = new HashMap<>();
        this.totalNumElementsPerDataRange = new HashMap<>();
    }

    public void clear() {
        this.joinMatchesPerDataRangePair.clear();
        this.totalNumElementsPerDataRange.clear();
        actualSampleSizeA = 0;
        actualSampleSizeB = 0;
    }

    @Override
    public String toString() {
        return "JoinDataDistrStats{" +
                "joinMatchesPerDataRangePair=" + joinMatchesPerDataRangePair +
                ", totalNumElementsPerDataRange=" + totalNumElementsPerDataRange +
                ", actualSampleSizeA=" + actualSampleSizeA +
                ", actualSampleSizeB=" + actualSampleSizeB +
                ", targetSampleSizeA=" + targetSampleSizeA +
                ", targetSampleSizeB=" + targetSampleSizeB +
                '}';
    }
}