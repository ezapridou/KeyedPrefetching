package net.michaelkoepf.spegauge.api.sut;

import java.util.HashMap;
import java.util.HashSet;

public interface JoinMonitor {
    JoinDataDistrMergeStats getJoinDataDistrMergeStats();

    void enableMonitoringMerge(HashMap<Integer, HashSet<Integer>> activeQueriesToDataRanges,
                               DataRange[] dataRanges, HashSet<Integer> activeQueries);

    void disableMonitoringMerge();

    void enableMonitoringSplit();

    void disableMonitoringSplit();

    DataDistrSplitStats getSplitMonitoringStats();
}
