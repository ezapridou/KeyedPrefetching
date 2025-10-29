package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordWithQuerySet;
import net.michaelkoepf.spegauge.api.sut.DataRange;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;

/**
 * This QuerySetAssigner is used to for queries that have a filter before the shared part.
 * It checks to which queries this record belongs and sets the querySet accordingly.
 */
public class QuerySetAssignerDataRange<T extends EntityRecordWithQuerySet> extends QuerySetAssignerImpl<T>{
    DataRange[] dataRanges;

    public QuerySetAssignerDataRange(int initialActiveQuery, int numOfQueries, DataRange[] dataRanges) {
        super(initialActiveQuery, numOfQueries);
        assert dataRanges.length == numOfQueries;
        this.dataRanges = dataRanges;
    }

    @Override
    public void flatMap(T record, Collector<T> collector) throws IOException {
        filterAndAssignQuerySet(record, collector);
    }

    protected void filterAndAssignQuerySet(T record, Collector<T> collector) {
        boolean atLeastOneActive = false;
        for (int i=0; i<dataRanges.length; i++) {
            if (activeQueries.get(i) && record.filterAttribute >= dataRanges[i].start && record.filterAttribute <= dataRanges[i].end) {
                record.querySet.set(i);
                atLeastOneActive = true;
            }
        }
        if (atLeastOneActive) {
            collector.collect(record);
        }
    }
}
