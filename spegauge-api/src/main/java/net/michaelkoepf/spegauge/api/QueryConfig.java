package net.michaelkoepf.spegauge.api;

public final class QueryConfig {
    // enum with all supported queries
    public enum QUERY_NAME {
        QUERY13, QUERY18, QUERY19, QUERY20, YSB, QUERY13_PREFETCHING, QUERY18_PREFETCHING, QUERY19_PREFETCHING,
        QUERY20_PREFETCHING, YSB_PREFETCHING, JSON_ETL_JOIN_QUERY, JSON_ETL_JOIN_QUERY_PREFETCHING
    }

    public static final QUERY_NAME SELECTED_QUERY = QUERY_NAME.QUERY13_PREFETCHING;

    public enum BENCHMARK_TYPE {
        SQBENCH_DEFAULT, NEXMARK, YSB
    }

    public static final BENCHMARK_TYPE SELECTED_BENCHMARK = BENCHMARK_TYPE.NEXMARK;

    public static final int PARALLELISM = 48;

    public static final int MAX_PARALLELISM = 192;

    public static final String REDIS_HOST = "10.90.46.32";
    public static final int REDIS_PORT = 6379;
}
