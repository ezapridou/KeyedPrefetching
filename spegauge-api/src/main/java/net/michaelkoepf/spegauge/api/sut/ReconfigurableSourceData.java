package net.michaelkoepf.spegauge.api.sut;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class ReconfigurableSourceData implements Serializable {
    private final String driverHostname;
    private final int driverDataGeneratorPort;
    private final String driverJobId;
    private final int driverSubTaskIdx;
    private final long lastEventId;
}
