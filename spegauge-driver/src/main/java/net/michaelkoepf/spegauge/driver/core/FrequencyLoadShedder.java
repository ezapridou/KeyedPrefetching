package net.michaelkoepf.spegauge.driver.core;

import net.michaelkoepf.spegauge.api.driver.LoadShedder;
import net.michaelkoepf.spegauge.api.driver.SheddableEvent;
import net.michaelkoepf.spegauge.driver.Constants;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// TODO: implement
public class FrequencyLoadShedder implements LoadShedder {

    private Logger LOGGER = LoggerFactory.getLogger(FrequencyLoadShedder.class);

    private TelemetryObject telemetryObject;
    private final RandomSource rng;

    private RestorableUniformRandomProvider random;

    private double sheddingRate;

    private long desiredThroughputPerSec;

    private int eventsSinceLastStep;

    private long lastThroughputMeasurementID;

    private double UPDATE_RATE = 0.5;

    // numOfTuples between measurements
    private long measurementsInterval = 10000;

    private long residual;

    private int subtaskIndex;

    private final double DISCOUNT_INIT_VALUE = 0.1;

    private final int DISCOUNT_INCREASE_RATE = 3;

    private double discount;


    // TODO the residual must be split among the tasks
    // also if the residual is less than the input rate maybe shed normally

    public FrequencyLoadShedder(Map<String, Object> params) {
        if (params.containsKey("rng")) {
            this.rng = (RandomSource) params.get("rng");
        }
        else {
            this.rng = RandomSource.XO_SHI_RO_256_PLUS;
        }

        if (params.containsKey("telemetryObject")) {
            this.telemetryObject = (TelemetryObject) params.get("telemetryObject");
        }
        if (params.containsKey("subtaskIndex")) {
            this.subtaskIndex = (int) params.get("subtaskIndex");
        }
        else {
            throw new IllegalArgumentException("subtaskIndex must be provided");
        }
        this.sheddingRate = 0;
        this.random = rng.create();
        this.desiredThroughputPerSec = 0;
        this.eventsSinceLastStep = 0;
        this.lastThroughputMeasurementID = Long.MIN_VALUE;
        this.residual = 0;
        this.discount = DISCOUNT_INIT_VALUE;
        this.subtaskIndex = subtaskIndex;
    }

    public void setTelemetryObject(TelemetryObject telemetryObject) {
        this.telemetryObject = telemetryObject;
    }

    @Override
    public boolean shed(SheddableEvent event) {
        long newDesiredThroughput = event.getSheddingInformation().getDesiredThroughputPerSecond();
        // desired throughput changed
        if (newDesiredThroughput != desiredThroughputPerSec) {
            double sustainableThroughput = sheddingRate == 0 ? newDesiredThroughput : desiredThroughputPerSec * (1 - sheddingRate);
            sheddingRate = Math.min(1.0, 1 - (sustainableThroughput / newDesiredThroughput));
            desiredThroughputPerSec = newDesiredThroughput;
            measurementsInterval = ((newDesiredThroughput * Constants.REPORTING_INTERVAL_MS / 1000) / telemetryObject.getNumOfDataGenerators()) / 10;
            LOGGER.info("measurementsInterval: " + measurementsInterval);
        }
        // positive residual means that we are lacking behind the desired input rate
        // we must shed to catch up
        if (residual > desiredThroughputPerSec/telemetryObject.getNumOfDataGenerators()) {
            residual--;
            return true;
        }
        // negative residual means that we are ahead of the desired input rate
        else if (residual < (-1) * desiredThroughputPerSec/telemetryObject.getNumOfDataGenerators()) {
            residual++;
            return false;
        }
        return random.nextDouble() < sheddingRate;
    }

    @Override
    public LoadShedder copy() {
        // TODO: don't we want to keep the history of the shedder?
        return new FrequencyLoadShedder(Map.of("telemetryObject", telemetryObject, "rng", rng, "subtaskIndex", subtaskIndex));
    }

    @Override
    public void step() {
        eventsSinceLastStep++;
        if (eventsSinceLastStep >= measurementsInterval) {
            eventsSinceLastStep = 0;
            TelemetryObject.ThroughputMeasurementWithID throughputMeasurement = telemetryObject.getCurrentThroughputWithId();
            if (throughputMeasurement.id != lastThroughputMeasurementID) {
                if (lastThroughputMeasurementID > 3) {
                    long consumptionRate = telemetryObject.getMetricAggregated(
                            TelemetryMetricType.RawMetric.CURRENT_RATE_OF_DATA_CONSUMPTION);
                    residual = telemetryObject.getMetricBySubtaskIndex(TelemetryMetricType.RawMetric.CURRENT_BACKLOG, subtaskIndex);
                    double newSheddingRateCalc = Math.min(1.0, 1 - (((double) throughputMeasurement.throughput) / desiredThroughputPerSec));
                    if (residual <= 0) {
                        newSheddingRateCalc *=  (1 - discount);
                        if (newSheddingRateCalc != 0 && discount < 1.0) {
                            discount = Math.min(1, discount * DISCOUNT_INCREASE_RATE);
                        }
                    }
                    else {
                        discount = DISCOUNT_INIT_VALUE;
                    }
                    sheddingRate = Math.min(1.0, (1 - UPDATE_RATE) * sheddingRate + UPDATE_RATE * newSheddingRateCalc);
                    telemetryObject.setSheddingRate(sheddingRate, subtaskIndex);
                    LOGGER.info("shedRate: " + sheddingRate + " (inputRate: " + desiredThroughputPerSec + " tpout: " + throughputMeasurement.throughput + ")" + "ConsRate " + consumptionRate + " residual " + residual);
                }
                lastThroughputMeasurementID = throughputMeasurement.id;
            }
        }
    }
}
