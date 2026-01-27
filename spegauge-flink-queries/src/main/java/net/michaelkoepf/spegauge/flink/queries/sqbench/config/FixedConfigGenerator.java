package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FixedConfigGenerator {
    // the main is used just to be able to check out the generated config
    public static void main(String[] args) throws JsonProcessingException {
        QueryConfig queryConfig = getConfig();
    }

    public static QueryConfig getConfig() {
        return getConfigQuery20();
    }

    public static QueryConfig getConfig(int selectivityOutOf100) {
        return getConfigSel(selectivityOutOf100);
    }

    public static QueryConfig getConfigSel(int selectivityOutOf100) {
        int numOfQueries = 2;
        List<DataAnalyticsQuery> dataAnalyticsQueries = new ArrayList<>();
        double selectivity = selectivityOutOf100 / 100.0;
        int rangeStart = 0;
        int numDistinctValues = 100;
        int numElementsSelected = (int) (numDistinctValues * selectivity);
        int rangeEnd = rangeStart + numElementsSelected - 1;


        for (int query = 0; query < numOfQueries; query++) {
            //System.out.println("rangeStart: " + rangeStart + " and rangeEnd: " + rangeEnd + " for query " + query);
            DataAnalyticsQuery dataAnalyticsQuery = DataAnalyticsQuery.builder()
                    .withJoinOperators(WindowJoin.builder()
                            .withJoinType(WindowJoin.JoinType.TWO_WAY_EQUI_JOIN)
                            .withWindowSizeMs(60000)
                            .withWindowSlideMs(1000)
                            .withParallelismFactor(8)
                            .build())
                    /*.withDownstreamOperator(WindowAggregation.builder()
                            .withAggregationType(WindowAggregation.AggregationType.MEAN)
                            .withGroupByField("f0").build())*/
                    .withFilter(FilterSpec.builder()
                            .withStartForEntityA(rangeStart)
                            .withEndForEntityA(rangeEnd)
                            .withStartForEntityB(rangeStart)
                            .withEndForEntityB(rangeEnd)
                            .withStartForEntityC(Long.MIN_VALUE)
                            .withEndForEntityC(Long.MAX_VALUE)
                            .build())
                    .build();
            dataAnalyticsQueries.add(dataAnalyticsQuery);
        }

        // === QUERY CONFIG ===
        QueryConfig queryConfig = QueryConfig.builder()
                .withDataAnalyticsQueries(dataAnalyticsQueries)
//            .withStreamingETLQueries(List.of(streamingETLQuery1, streamingETLQuery2))
                .build();

        return queryConfig;

    }

    public static QueryConfig getConfigJsonETLJoinQuery() {
        List<DataAnalyticsQuery> dataAnalyticsQueries = new ArrayList<>();

        // === DATA ANALYTICS QUERIES ===
        DataAnalyticsQuery dataAnalyticsQuery = DataAnalyticsQuery.builder()
                .withJoinOperators(WindowJoin.builder()
                        .withJoinType(WindowJoin.JoinType.TWO_WAY_EQUI_JOIN)
                        .withWindowSizeMs(1800000)
                        .withWindowSlideMs(60000)
                        .withParallelismFactor(1)
                        .build())
                .withFilter(FilterSpec.builder()
                        .withStartForEntityA(Long.MIN_VALUE)
                        .withEndForEntityA(Long.MAX_VALUE)
                        .withStartForEntityB(Long.MIN_VALUE)
                        .withEndForEntityB(Long.MAX_VALUE)
                        .withStartForEntityC(Long.MIN_VALUE)
                        .withEndForEntityC(Long.MAX_VALUE)
                        .build())
                .build();
        dataAnalyticsQueries.add(dataAnalyticsQuery);

        // === QUERY CONFIG ===
        QueryConfig queryConfig = QueryConfig.builder()
                .withDataAnalyticsQueries(dataAnalyticsQueries)
                .build();

        return queryConfig;
    }

    public static QueryConfig getConfigQuery20() {
        List<DataAnalyticsQuery> dataAnalyticsQueries = new ArrayList<>();

        // === DATA ANALYTICS QUERIES ===
        DataAnalyticsQuery dataAnalyticsQuery = DataAnalyticsQuery.builder()
                .withJoinOperators(WindowJoin.builder()
                        .withJoinType(WindowJoin.JoinType.TWO_WAY_EQUI_JOIN)
                        .withWindowSizeMs(14400000) // 60 min
                        .withWindowSlideMs(14400000) // 1 min
                        .withParallelismFactor(1)
                        .build())
                .withFilter(FilterSpec.builder()
                        .withStartForEntityA(Long.MIN_VALUE)
                        .withEndForEntityA(Long.MAX_VALUE)
                        .withStartForEntityB(Long.MIN_VALUE)
                        .withEndForEntityB(Long.MAX_VALUE)
                        .withStartForEntityC(Long.MIN_VALUE)
                        .withEndForEntityC(Long.MAX_VALUE)
                        .build())
                .build();
        dataAnalyticsQueries.add(dataAnalyticsQuery);

        // === QUERY CONFIG ===
        QueryConfig queryConfig = QueryConfig.builder()
                .withDataAnalyticsQueries(dataAnalyticsQueries)
                .build();

        return queryConfig;
    }

    public static QueryConfig getConfigDifferentSelectivities() {
        int numOfQueries = 32;
        int domainStart = 0;
        int numDistinctValues = 100;
        List<DataAnalyticsQuery> dataAnalyticsQueries = new ArrayList<>();

        Random random = new Random(19L);
        // === DATA ANALYTICS QUERIES ===
        // [\"statefuljoingid0\",\"statefuljoingid1\",\"statefuljoingid2\",\"statefuljoingid3\"]
        //{\"0\":[0,9],\"1\":[0,9],\"2\":[0,9],\"3\":[0,9]}
        String print = "[";
        String print2 = "{";
        for (int query = 0; query < numOfQueries; query++) {
            double selectivity = (random.nextInt(100) + 1) / 100.0; // between 0.1 and 1.0
            int numElementsSelected = (int) (numDistinctValues * selectivity);
            int rangeStart = domainStart;
            int rangeEnd = rangeStart + numElementsSelected - 1;
            System.out.println("rangeStart: " + rangeStart + " and rangeEnd: " + rangeEnd
                    + " selectivity: " + selectivity + " for query " + query);
            DataAnalyticsQuery dataAnalyticsQuery = DataAnalyticsQuery.builder()
                    .withJoinOperators(WindowJoin.builder()
                            .withJoinType(WindowJoin.JoinType.TWO_WAY_EQUI_JOIN)
                            .withWindowSizeMs(60000)
                            .withWindowSlideMs(1000)
                            .withParallelismFactor(32)
                            .build())
                    //.withDownstreamOperator(RollingReduce.builder().withAggregationType(RollingReduce.AggregationType.MAX).withGroupByField("f0").build())
                    .withFilter(FilterSpec.builder()
                            .withStartForEntityA(rangeStart)
                            .withEndForEntityA(rangeEnd)
                            .withStartForEntityB(rangeStart)
                            .withEndForEntityB(rangeEnd)
                            .withStartForEntityC(Long.MIN_VALUE)
                            .withEndForEntityC(Long.MAX_VALUE)
                            .build())
                    .build();
            dataAnalyticsQueries.add(dataAnalyticsQuery);
            if (query == numOfQueries - 1) {
                print += "\\\"statefuljoingid" + query + "\\\"";
                print2 += "\\\"" + query + "\\\":[" + rangeStart + "," + rangeEnd + "]";
            } else {
                print += "\\\"statefuljoingid" + query + "\\\",";
                print2 += "\\\"" + query + "\\\":[" + rangeStart + "," + rangeEnd + "],";
            }
        }
        print += "]";
        print2 += "}";
        System.out.println(print);
        System.out.println(print2);

        // === QUERY CONFIG ===
        QueryConfig queryConfig = QueryConfig.builder()
                .withDataAnalyticsQueries(dataAnalyticsQueries)
                .build();

        return queryConfig;
    }

    public static QueryConfig getConfigSameSelectivities() {
        int numOfQueries = 32;
        double selectivity = 0.1;
        int domainStart = 0;
        int numDistinctValues = 100;
        List<DataAnalyticsQuery> dataAnalyticsQueries = new ArrayList<>();
        int numElementsSelected = (int) (numDistinctValues * selectivity);
        int maxRangeStart = domainStart + numDistinctValues - numElementsSelected;
        Random random = new Random(19L);
        // === DATA ANALYTICS QUERIES ===
        // [\"statefuljoingid0\",\"statefuljoingid1\",\"statefuljoingid2\",\"statefuljoingid3\"]
        //{\"0\":[0,9],\"1\":[0,9],\"2\":[0,9],\"3\":[0,9]}
        String print = "[";
        String print2 = "{";
        for (int query = 0; query < numOfQueries; query++) {
            int rangeStart = random.nextInt(maxRangeStart - domainStart + 1) + domainStart;
            int rangeEnd = rangeStart + numElementsSelected - 1;
            //System.out.println("rangeStart: " + rangeStart + " and rangeEnd: " + rangeEnd + " for query " + query);
            DataAnalyticsQuery dataAnalyticsQuery = DataAnalyticsQuery.builder()
                    .withJoinOperators(WindowJoin.builder()
                            .withJoinType(WindowJoin.JoinType.TWO_WAY_EQUI_JOIN)
                            .withWindowSizeMs(60000)
                            .withWindowSlideMs(1000)
                            .withParallelismFactor(32)
                            .build())
                    //.withDownstreamOperator(RollingReduce.builder().withAggregationType(RollingReduce.AggregationType.MAX).withGroupByField("f0").build())
                    .withFilter(FilterSpec.builder()
                            .withStartForEntityA(rangeStart)
                            .withEndForEntityA(rangeEnd)
                            .withStartForEntityB(rangeStart)
                            .withEndForEntityB(rangeEnd)
                            .withStartForEntityC(Long.MIN_VALUE)
                            .withEndForEntityC(Long.MAX_VALUE)
                            .build())
                    .build();
            dataAnalyticsQueries.add(dataAnalyticsQuery);
            if (query == numOfQueries - 1) {
                print += "\\\"statefuljoingid" + query + "\\\"";
                print2 += "\\\"" + query + "\\\":[" + rangeStart + "," + rangeEnd + "]";
            } else {
                print += "\\\"statefuljoingid" + query + "\\\",";
                print2 += "\\\"" + query + "\\\":[" + rangeStart + "," + rangeEnd + "],";
            }
        }
        print += "]";
        print2 += "}";
        System.out.println(print);
        System.out.println(print2);

        // === STREAMING ETL QUERIES ===
        /*var streamingETLQuery1 = StreamingETLQuery.builder()
                .withExtractOperation(ExtractOperator.builder().withParallelismFactor(2).withOperationType(ExtractOperator.OperationType.EXTRACT_JSON).build())
                .withDownstreamOperator(DataEnrichtmentFunction.builder().withFilterField(EntityRecordFull.Type.A).withEnrichmentType(DataEnrichtmentFunction.EnrichmentType.SIMULATED_ENRICHMENT).build()).build();

        var streamingETLQuery2 = StreamingETLQuery.builder()
                .withExtractOperation(ExtractOperator.builder().withOperationType(ExtractOperator.OperationType.EXTRACT_JSON).build())
                .withDownstreamOperator(WindowedSumAndPOJOToX.builder().withFilterField(EntityRecordFull.Type.B).withWindowSizeMs(3000).withWindowSlideMs(1000).withTargetType(WindowedSumAndPOJOToX.TargetType.POJO_TO_AVRO).build()).build();
        */

        // === QUERY CONFIG ===
        QueryConfig queryConfig = QueryConfig.builder()
                .withDataAnalyticsQueries(dataAnalyticsQueries)
//            .withStreamingETLQueries(List.of(streamingETLQuery1, streamingETLQuery2))
                .build();

        return queryConfig;
    }
}
