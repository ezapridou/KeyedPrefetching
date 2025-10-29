package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;

import java.util.List;

/**
 * Builder for building a configuration programmatically (just run in your IDE).
 */
public class ConfigGenerator {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final ObjectWriter objectWriter = objectMapper.writer().withDefaultPrettyPrinter();

  public static void main(String[] args) throws JsonProcessingException {
    // TODO: adapt as needed an run in your IDE. The generated JSON is written to stdout.

    // === DATA ANALYTICS QUERIES ===
    var dataAnalyticsQuery1 = DataAnalyticsQuery.builder()
            .withJoinOperators(WindowJoin.builder()
                    .withJoinType(WindowJoin.JoinType.TWO_WAY_EQUI_JOIN)
                    .withWindowSizeMs(3000)
                    .withWindowSlideMs(3000)
                    .withParallelismFactor(2)
                    .build())
            .withDownstreamOperator(RollingReduce.builder().withAggregationType(RollingReduce.AggregationType.MAX).withGroupByField("f0").build())
            /* .withFilter(FilterSpec.builder()
                    .withStartForEntityA(0)
                    .withEndForEntityA(100)
                    .withStartForEntityB(Long.MIN_VALUE)
                    .withEndForEntityB(Long.MAX_VALUE)
                    .withStartForEntityC(Long.MIN_VALUE)
                    .withEndForEntityC(Long.MAX_VALUE)
                    .build()) */
            .build();

    var dataAnalyticsQuery2 = DataAnalyticsQuery.builder()
            .withJoinOperators(WindowJoin.builder()
                    .withJoinType(WindowJoin.JoinType.TWO_WAY_EQUI_JOIN)
                    .withWindowSizeMs(3000)
                    .withWindowSlideMs(3000)
                    .build())
            /* .withFilter(FilterSpec.builder()
                    .withStartForEntityA(0)
                    .withEndForEntityA(100)
                    .withStartForEntityB(Long.MIN_VALUE)
                    .withEndForEntityB(Long.MAX_VALUE)
                    .withStartForEntityC(Long.MIN_VALUE)
                    .withEndForEntityC(Long.MAX_VALUE)
                    .build()) */
            .withDownstreamOperator(WindowAggregation.builder().withAggregationType(WindowAggregation.AggregationType.MEDIAN).withWindowSizeMs(3000).withWindowSlideMs(1000).withGroupByField("f1").build()).build();

    // === STREAMING ETL QUERIES ===
    var streamingETLQuery1 = StreamingETLQuery.builder()
            .withExtractOperation(ExtractOperator.builder().withParallelismFactor(2).withOperationType(ExtractOperator.OperationType.EXTRACT_JSON).build())
            .withDownstreamOperator(DataEnrichtmentFunction.builder().withFilterField(EntityRecordFull.Type.A).withEnrichmentType(DataEnrichtmentFunction.EnrichmentType.SIMULATED_ENRICHMENT).build()).build();

    var streamingETLQuery2 = StreamingETLQuery.builder()
            .withExtractOperation(ExtractOperator.builder().withOperationType(ExtractOperator.OperationType.EXTRACT_JSON).build())
            .withDownstreamOperator(WindowedSumAndPOJOToX.builder().withFilterField(EntityRecordFull.Type.B).withWindowSizeMs(3000).withWindowSlideMs(1000).withTargetType(WindowedSumAndPOJOToX.TargetType.POJO_TO_AVRO).build()).build();


    // === QUERY CONFIG ===
    var queryConfig = QueryConfig.builder()
            .withDataAnalyticsQueries(List.of(dataAnalyticsQuery1, dataAnalyticsQuery2))
//            .withStreamingETLQueries(List.of(streamingETLQuery1, streamingETLQuery2))
            .build();

    String json = objectWriter.writeValueAsString(queryConfig);

    System.out.println(json); // prints the JSON configuration to the console
  }
}
