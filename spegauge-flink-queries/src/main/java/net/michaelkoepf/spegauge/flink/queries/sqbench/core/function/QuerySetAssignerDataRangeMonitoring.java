package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordLimitedAttrs;
import net.michaelkoepf.spegauge.api.sut.BitSet;
import net.michaelkoepf.spegauge.api.sut.DataRange;
import net.michaelkoepf.spegauge.api.sut.FilterDataDistrMergeStats;
import net.michaelkoepf.spegauge.api.sut.SelectivitiesMonitor;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class QuerySetAssignerDataRangeMonitoring<T extends EntityRecordLimitedAttrs> extends QuerySetAssignerDataRange<T> implements SelectivitiesMonitor {
    FilterDataDistrMergeStats monitoringStats;
    boolean monitoringEnabled;
    DataRange[] monitoringDataRanges;
    HashMap<Integer, HashSet<Integer>> queryToDataRangeMap;

    public QuerySetAssignerDataRangeMonitoring(int initialActiveQuery, int numOfQueries, DataRange[] dataRanges) {
        super(initialActiveQuery, numOfQueries, dataRanges);
        this.monitoringStats = new FilterDataDistrMergeStats();
        this.monitoringEnabled = false;
    }

    @Override
    public void flatMap(T record, Collector<T> collector) throws IOException {
        if (monitoringEnabled) {
            filterAndAssignQuerySetMonitoring(record, collector);
        }
        else {
            filterAndAssignQuerySet(record, collector);
        }
    }

    private void filterAndAssignQuerySetMonitoring(T record, Collector<T> collector) {
        boolean atLeastOneActive = false;
        monitoringStats.totalNumElements++;
        for (int i=0; i<monitoringDataRanges.length; i++) {
            if (record.filterAttribute >= monitoringDataRanges[i].start && record.filterAttribute <= monitoringDataRanges[i].end) {
                long statsForDataRange =  monitoringStats.selectivityPerDataRange
                        .computeIfAbsent(monitoringDataRanges[i], x -> 0L);
                monitoringStats.selectivityPerDataRange.put(monitoringDataRanges[i], statsForDataRange + 1);

                // during monitoring we abuse the querySet to store the data range index
                if (record.querySet.size() < monitoringDataRanges.length) {
                    record.querySet = new BitSet(monitoringDataRanges.length);
                }
                record.querySet.set(i);

                atLeastOneActive = true;
                // we can break since monitoringDataRanges are non-overlapping
                break;
            }
        }
        if (atLeastOneActive) {
            collector.collect(record);
        }
    }

    public FilterDataDistrMergeStats getFilterDataDistrStats() {
        this.monitoringEnabled = false;
        System.out.println("Monitoring disabled in filter operator.");
        return monitoringStats;
    }

    public void enableMonitoring(HashMap<Integer, HashSet<Integer>> queryToDataRangesMap,
                                 DataRange[] dataRanges) {
        this.monitoringEnabled = true;
        this.monitoringStats.clear();
        this.monitoringDataRanges = dataRanges;
        this.queryToDataRangeMap = queryToDataRangesMap;
        System.out.println("Monitoring enabled in filter operator. " + monitoringDataRanges.toString());
    }
}
