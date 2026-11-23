# Aqueduct

![build workflow](https://github.com/pranay/aqueduct/actions/workflows/build.yml/badge.svg)
![package workflow](https://github.com/pranay/aqueduct/actions/workflows/package.yml/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache)](LICENSE)
[![Version](https://img.shields.io/github/v/release/pranay/aqueduct?logo=semantic-release)](Version)

Aqueduct is a cloud native service for delivering real-time streaming data to destinations such as service endpoints (HTTP or GRPC) & managed databases (Postgres, InfluxDB, Redis, Elasticsearch, Prometheus and MongoDB). With Aqueduct, you don't need to write applications or manage resources. It can be scaled up to match the throughput of your data. If your data is present in Kafka, Aqueduct delivers it to the destination(SINK) that you specified.

<p align="center"><img src="./docs/static/assets/overview.svg" /></p>

## Key Features

Discover why users choose Aqueduct as their main Kafka Consumer

- **Sinks:** Aqueduct supports sinking stream data to :

  - log console
  - MongoDB
  - Prometheus
  - HTTP
  - GRPC
  - PostgresDB(JDBC)
  - InfluxDB
  - Elasticsearch
  - Redis
  - Bigquery
  - BigTable
  - Blob Storage/Object Storage :
    - Google Cloud Storage

- **Scale:** Aqueduct scales in an instant, both vertically and horizontally for high performance streaming sink and zero data drops.
- **Extensibility:** Add your own sink to aqueduct with a clearly defined interface or choose from already provided ones.
- **Runtime:** Aqueduct can run inside VMs or containers in a fully managed runtime environment like kubernetes.
- **Metrics:** Always know what’s going on with your deployment with built-in [monitoring](./docs/assets/aqueduct-grafana-dashboard.json) of throughput, response times, errors and more.

To know more, follow the detailed [documentation](docs)

## Usage

Explore the following resources to get started with Aqueduct:

- [Guides](docs/docs/guides) provides guidance on [creating Aqueduct](docs/guides/overview.md) with different sinks.
- [Concepts](docs/docs/concepts) describes all important Aqueduct concepts.
- [Reference](docs/docs/reference) contains details about configurations, metrics and other aspects of Aqueduct.
- [Contribute](docs/docs/contribute/contribution.md) contains resources for anyone who wants to contribute to Aqueduct.

## Run with Docker

Use the docker hub to download aqueduct [docker image](https://hub.docker.com/r/pranay/aqueduct/). You need to have docker installed in your system.

```
# Download docker image from docker hub
$ docker pull pranay/aqueduct

# Run the following docker command for a simple log sink.
$ docker run -e SOURCE_KAFKA_BROKERS=127.0.0.1:6667 -e SOURCE_KAFKA_CONSUMER_GROUP_ID=kafka-consumer-group-id -e SOURCE_KAFKA_TOPIC=sample-topic -e SINK_TYPE=log -e SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET=latest -e INPUT_SCHEMA_PROTO_CLASS=com.github.aqueduct.sampleLogProto.SampleLogMessage -e SCHEMA_REGISTRY_STENCIL_ENABLE=true -e SCHEMA_REGISTRY_STENCIL_URLS=http://localhost:9000/artifactory/proto-descriptors/latest/pranay/aqueduct:latest
```

**Note:** Make sure your protos (.jar file) are located in `work-dir`, this is required for Filter functionality to work.

## Run with Kubernetes

- Create a aqueduct deployment using the helm chart available [here](https://github.com/pranay/charts/tree/main/stable/aqueduct)
- Deployment also includes telegraf container which pushes stats metrics

## Running locally

```sh
# Clone the repo
$ git clone https://github.com/pranay/aqueduct.git

# Build the jar
$ ./gradlew clean build

# Configure env variables
$ cat env/local.properties

# Run the Aqueduct
$ ./gradlew runConsumer
```

**Note:** Sample configuration for other sinks along with some advanced configurations can be found [here](/docs/reference/configuration.md)

## Running tests

```sh
# Running unit tests
$ ./gradlew test

# Run code quality checks
$ ./gradlew checkstyleMain checkstyleTest

#Cleaning the build
$ ./gradlew clean
```

## Contribute

Development of Aqueduct happens in the open on GitHub, and we are grateful to the community for contributing bugfixes and improvements. Read below to learn how you can take part in improving Aqueduct.

Read our [contributing guide](docs/docs/contribute/contribution.md) to learn about our development process, how to propose bugfixes and improvements, and how to build and test your changes to Aqueduct.

To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/pranay/aqueduct/labels/good%20first%20issue) that contain bugs which have a relatively limited scope. This is a great place to get started.

## Credits

This project exists thanks to all the [contributors](https://github.com/pranay/aqueduct/graphs/contributors).

## License

Aqueduct is [Apache 2.0](LICENSE) licensed.
