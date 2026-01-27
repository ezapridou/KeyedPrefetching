package net.michaelkoepf.spegauge.flink.queries.nexmark.core;

import com.codahale.metrics.UniformReservoir;
import net.michaelkoepf.spegauge.api.common.model.nexmark.Event;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/** Implementation of Nexmark query 1 */
public class FilterMicroBenchmark extends QueryGroup<Event> {
    @Override
    public List<DataStream> register(StreamExecutionEnvironment env)
            throws IOException {
        DataStream<Event> source =
                QueryUtils.newSourceStream(
                                env, getHostname(), getPort(), getSourceParallelism())
                        .rebalance();

        DataStream<Event> bids = QueryUtils.filterBids(source, getParallelism());

        DataStream<Event> result =  source
                .filter(new RichFilterFunction<Event>() {
                    private transient Histogram histogram;

                    @Override
                    public boolean filter(Event event) throws Exception {
                        long start = System.nanoTime();
                        boolean filter = (event.type == Event.Type.BID);
                        this.histogram.update(System.nanoTime()-start);
                        return filter;
                    }

                    @Override
                    public void open(OpenContext config) {
                        com.codahale.metrics.Histogram dropwizardHistogram = new com.codahale.metrics.Histogram(new UniformReservoir());
                        this.histogram = getRuntimeContext()
                                .getMetricGroup()
                                .histogram("filterMicroBenchmarkHist", new DropwizardHistogramWrapper(dropwizardHistogram));
                    }
                })
                .name("filterMicroBenchmark");

        return new LinkedList<DataStream>() {
            {
                add(result);
            }
        };

    }

    @Override
    public String getName() {
        return "FILTER_MICRO_BENCHMARK";
    }
}
