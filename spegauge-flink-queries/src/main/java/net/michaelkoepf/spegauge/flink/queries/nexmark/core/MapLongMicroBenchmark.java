package net.michaelkoepf.spegauge.flink.queries.nexmark.core;

import com.codahale.metrics.UniformReservoir;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.api.common.model.nexmark.Event;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/** Implementation of Nexmark query 1 */
public class MapLongMicroBenchmark extends QueryGroup<Tuple2<Long, Long>> {
    @Override
    public List<DataStream> register(StreamExecutionEnvironment env)
            throws IOException {
        DataStream<Event> source =
                QueryUtils.newSourceStream(
                                env, getHostname(), getPort(), getSourceParallelism())
                        .rebalance();

        DataStream<Event> bids = QueryUtils.filterBids(source, getParallelism());

        DataStream<Tuple2<Long, Long>> result =
                bids.map(new RichMapFunction<Event, Tuple2<Long, Long>>() {
                            private transient Histogram histogram;

                            @Override
                            public Tuple2<Long, Long> map(Event event) throws Exception {
                                long start = System.nanoTime();
                                var tuple = new Tuple2<>(event.bid.auction, Math.round((event.bid.price * 0.92)));
                                this.histogram.update(System.nanoTime()-start);
                                return tuple;
                            }

                            @Override
                            public void open(OpenContext config) {
                                com.codahale.metrics.Histogram dropwizardHistogram = new com.codahale.metrics.Histogram(new UniformReservoir());
                                this.histogram = getRuntimeContext()
                                        .getMetricGroup()
                                        .histogram("mapLongMicroBenchmarkHist", new DropwizardHistogramWrapper(dropwizardHistogram));
                            }
                        })
                        .returns(TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}))
                        .name("mapLongMicroBenchmark");

        return new LinkedList<DataStream>() {
            {
                add(result);
            }
        };

    }

    @Override
    public String getName() {
        return "MAP_LONG_MICRO_BENCHMARK";
    }
}
