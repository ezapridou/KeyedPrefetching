package net.michaelkoepf.spegauge.flink.queries.nexmark.core;

import com.codahale.metrics.UniformReservoir;
import net.michaelkoepf.spegauge.api.common.model.nexmark.Event;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

public class LoadTest extends QueryGroup<Event> {

  @Override
  public List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
    DataStream<Event> source =
        QueryUtils.newSourceStream(
                env, getHostname(), getPort(), getSourceParallelism())
            .rebalance();

    DataStream<Event> result = source.map(new RichMapFunction<Event, Event>() {
      private transient Histogram histogram;

      @Override
      public Event map(Event value) throws Exception {
        long start = System.nanoTime();
        long end = System.nanoTime();
        this.histogram.update(end-start);
        return value;
      }

      @Override
      public void open(OpenContext config) {
        com.codahale.metrics.Histogram dropwizardHistogram = new com.codahale.metrics.Histogram(new UniformReservoir());
        this.histogram = getRuntimeContext()
                .getMetricGroup()
                .histogram("myHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));
      }
    });

    return new LinkedList<DataStream>() {
      {
        add(result);
      }
    };
  }

  @Override
  public String getName() {
    return "Load Test";
  }
}
