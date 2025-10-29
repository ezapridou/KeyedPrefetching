package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordWithQuerySet;
import net.michaelkoepf.spegauge.api.sut.BitSet;
import net.michaelkoepf.spegauge.api.sut.QuerySetAssigner;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class QuerySetAssignerImpl<T extends EntityRecordWithQuerySet> implements QuerySetAssigner, FlatMapFunction<T, T> {
    // these are the queries that are currently using this plan
    protected volatile BitSet activeQueries;

    public QuerySetAssignerImpl(int initialActiveQuery, int numOfQueries) {
        this.activeQueries = new BitSet(numOfQueries);
        this.activeQueries.set(initialActiveQuery);
    }

    @Override
    public void flatMap(T record, Collector<T> collector) throws IOException {
        copyBitSet(this.activeQueries, record.querySet);
        collector.collect(record);
    }

    @Override
    public void setQueryActive(int queryId) {
        this.activeQueries.set(queryId);
    }

    @Override
    public void setQueryInactive(int queryId) {
        this.activeQueries.clear(queryId);
    }

    @Override
    public void setActiveQueries(boolean[] activeQueries) {
        copyBitSet(activeQueries, this.activeQueries);
    }

    private void copyBitSet(BitSet source, BitSet target) {
        for (int i = 0; i < source.size() || i < target.size(); i++) {
            if (source.get(i)) {
                target.set(i);
            }
            else {
                target.clear(i);
            }
        }
    }

    private void copyBitSet(boolean[] source, BitSet target) {
        for (int i = 0; i < source.length || i < target.size(); i++) {
            if (source[i]) {
                target.set(i);
            }
            else {
                target.clear(i);
            }
        }
    }
}
