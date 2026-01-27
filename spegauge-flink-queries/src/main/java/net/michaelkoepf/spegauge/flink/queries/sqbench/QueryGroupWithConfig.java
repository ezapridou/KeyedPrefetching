package net.michaelkoepf.spegauge.flink.queries.sqbench;

import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;

public abstract class QueryGroupWithConfig extends QueryGroup<Object> {

    public DataAnalyticsQuery queryConfig;

    public void setQueryConfig(DataAnalyticsQuery thisQuery) {
        this.queryConfig = thisQuery;
    }
}
