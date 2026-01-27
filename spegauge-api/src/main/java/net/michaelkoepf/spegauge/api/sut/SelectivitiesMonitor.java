package net.michaelkoepf.spegauge.api.sut;

import java.util.HashMap;
import java.util.HashSet;

public interface SelectivitiesMonitor {
    FilterDataDistrMergeStats getFilterDataDistrStats();
    void enableMonitoring(HashMap<Integer, HashSet<Integer>> queryToDataRangesMap,
                          DataRange[] dataRanges);
}
