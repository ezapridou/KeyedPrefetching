# Keyed Prefetching
The changes we made on the Flink backend are reflected on this repository: [KeyedPrefetching-Flink](https://github.com/ezapridou/KeyedPrefetching-Flink).

We reuse code for the data generation and query submission infrastructure from this repository: [SPEGauge](https://github.com/michaelkoepf/SPEGauge)

## Project Overview

```
├── LICENSE
├── README.md                                       # this file
├── mvnw                                            # Maven wrapper (*nix systems)
├── mvnw.cmd                                        # Maven wrapper (Windows)
├── pom.xml                                         # Parent pom
├── requirements.txt                                # Dependencies for all Python applications in this repository
├── example-configurations                          # Configuration examples for supported datasets (JSON files)
├── spegauge-api                                    # Common API that defines interfaces for data generators and events (Java). Used in spegauge-driver and spegauge-generators and all SDKs. 
├── spegauge-dashboard                              # Web dashboard to visualize metrics of the driver (Python) 
├── spegauge-driver                                 # Driver application (Java)
├── spegauge-flink-queries                          # Queries for benchmarking apache Flink (Java)
├── spegauge-flink-sdk                              # SDK for Apache Flink (Java)
├── spegauge-generators                             # A set of provided data generators (Java) and a validator for the configuration files (Python)
```

### `spegauge-api`

#### `common.model`

Data models (used in data generator and in the system under test SDKs)

#### `driver`

Interfaces implemented by event generators and used in the driver.

- `Event`: basic unit during event generation
- `EventGenerator`: Iterable container of events
- `EventGeneratorFactory`: returns a bound or unbound number of `EventGenerator` instances (but at least one for each generator instance in the driver)


### `spegauge-driver`

Driver application. 

### `spegauge-flink-queries`

Queries for benchmarking Apache Flink (e.g., NEXMark).

### `spegauge-flink-sdk`

SDK for Apache Flink. To use a source or sink with a new dataset, extend the abstract classes and implement the corresponding hooks (abstract methods).

### `spegauge-generators`

Provided data generators that implement the interfaces in `spegauge-api`. The path to the JAR is passed as argument to the driver application (see below)



### Compile and Package the Project

**WHERE?** packaging locally on your machine and cluster; copy command on cluster
  
- Package all JARs

  ```bash
  ./mvnw clean package
  ```

- Copy `spegauge-api/target/spegauge-api-0.1.0-SNAPSHOT.jar` and `spegauge-flink-sdk/target/spegauge-flink-sdk-0.1.0-SNAPSHOT.jar` to the `lib/` directory of your Apache Flink application.
- Restart the Apache Flink cluster, if it is running


## Examples

### NEXMark

#### Manual Job Submission

**WHERE?** cluster

- Run the driver application (16 parallel generators, 25M events per generator). The last line on the console should be `[main] INFO io.javalin.Javalin - Javalin started in ...ms \o/`.

  ```bash
  java -jar spegauge-driver/target/spegauge-driver-0.1.0-SNAPSHOT.jar -g 16 spegauge-generators/target/spegauge-generators-0.1.0-SNAPSHOT.jar
  ```

- Submit a POST request to the driver's web API (`/jobs`), e.g., using Postman (see screenshot below). The request contains the details for data generation (see `data-config/nexmark-example-config.json` for an example).

- Submit a job to the flink cluster (in this example, NEXMark query 4 is used)

  ```bash
  flink run -p 8 spegauge-flink-queries/target/spegauge-flink-queries-0.1.0-SNAPSHOT.jar -m nexmark --driver diascld21.iccluster.epfl.ch:8110 -- Query4
  ```

##### Driver Application

**WHERE?** cluster

- Run the driver application (16 parallel generators, 25M events per generator). The last line on the console should be `[main] INFO io.javalin.Javalin - Javalin started in ...ms \o/`.

  ```bash
  java -jar spegauge-driver/target/spegauge-driver-0.1.0-SNAPSHOT.jar -g 16 spegauge-generators/target/spegauge-generators-0.1.0-SNAPSHOT.jar
  ```

