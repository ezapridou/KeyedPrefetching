package net.michaelkoepf.spegauge.api.sut;

import java.io.Serializable;
import java.util.HashMap;

public class FilterDataDistrMergeStats implements Serializable {
    // per data range how many elements passed the filter
    public HashMap<DataRange, Long> selectivityPerDataRange;
    // total number of elements monitored
    public long totalNumElements;

    public FilterDataDistrMergeStats() {
        this.selectivityPerDataRange = new HashMap<>();
        this.totalNumElements = 0;
    }

    public void clear() {
        this.selectivityPerDataRange.clear();
        this.totalNumElements = 0;
    }

    @Override
    public String toString() {
        return "FilterDataDistrStats{" +
                "selectivityPerDataRange=" + selectivityPerDataRange +
                ", totalNumElements=" + totalNumElements +
                '}';
    }
}

