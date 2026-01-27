package net.michaelkoepf.spegauge.driver.webapi;

import lombok.Getter;
import lombok.Setter;
import net.michaelkoepf.spegauge.api.driver.Event;
import net.michaelkoepf.spegauge.api.driver.EventGeneratorFactory;
import net.michaelkoepf.spegauge.api.driver.LoadShedder;
import net.michaelkoepf.spegauge.driver.collection.EventContainer;
import net.michaelkoepf.spegauge.driver.core.AbstractController;
import net.michaelkoepf.spegauge.driver.core.TelemetryObject;

import java.util.List;
import java.util.concurrent.Future;

@Getter
public class DataGenerationJob {

    public enum Status {
        CREATED,
        NOT_ENOUGH_RESOURCES,
        WAITING_FOR_CLIENTS,
        CLIENTS_CONNECTED,
        TIMEOUT, // when clients do not connect in time
        RUNNING,
        CANCELLED,
        STALE_EVENTS,
        UNEXPECTED_ABORTION_BY_SUT,
        FINISHED,
        ILLEGAL_STATE,
        RECONFIGURATION,
        GRACEFULLY_TERMINATED_BY_SUT
    }

    private final AbstractController controller;

    private final List<EventContainer<Event>> queues;

    private final EventGeneratorFactory eventGeneratorFactory;

    private final TelemetryObject telemetryObject;

    private final List<LoadShedder> loadShedders;


    @Setter
    private Future<Long> controllerFuture;

    @Setter
    private Status status;

    @Setter
    private int port = -1;

    public DataGenerationJob(AbstractController controller, List<EventContainer<Event>> queues, TelemetryObject telemetryObject, EventGeneratorFactory eventGeneratorFactory, List<LoadShedder> loadShedders) {
        this.controller = controller;
        this.queues = queues;
        this.status = Status.CREATED;
        this.telemetryObject = telemetryObject;
        this.eventGeneratorFactory = eventGeneratorFactory;
        this.loadShedders = loadShedders;
    }
}
