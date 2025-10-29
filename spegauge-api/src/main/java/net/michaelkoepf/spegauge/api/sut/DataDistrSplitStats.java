package net.michaelkoepf.spegauge.api.sut;

import java.io.Serializable;
import java.util.HashMap;

public class DataDistrSplitStats implements Serializable {
    public HashMap<Integer, Integer> selectedNumElementsPerQuery;
    public HashMap<Integer, Integer> joinMatchesPerQuery;

    public final int targetSampleSizeA = 850;
    public final int targetSampleSizeB = 150;

    public int actualSampleSizeA = 0;
    public int actualSampleSizeB = 0;

    public DataDistrSplitStats() {
        this.selectedNumElementsPerQuery = new HashMap<>();
        this.joinMatchesPerQuery = new HashMap<>();
    }

    public void clear() {
        this.selectedNumElementsPerQuery.clear();
        this.joinMatchesPerQuery.clear();
        actualSampleSizeA = 0;
        actualSampleSizeB = 0;
    }

    @Override
    public String toString() {
        return "DataDistrSplitStats{" +
                "selectedNumElementsPerQuery=" + selectedNumElementsPerQuery +
                ", joinMatchesPerQuery=" + joinMatchesPerQuery +
                ", actualSampleSizeA=" + actualSampleSizeA +
                ", actualSampleSizeB=" + actualSampleSizeB +
                '}';
    }

}
