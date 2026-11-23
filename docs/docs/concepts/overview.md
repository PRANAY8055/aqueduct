# Overview

The following topics will describe key concepts of Aqueduct.

## [Architecture](architecture.md)

Aqueduct is a fully managed service that allows you to consume messages to any sink from kafka streams in realtime at
scale. This section explains the overall architecture of Aqueduct and describes its various components.

## [Monitoring Aqueduct with exposed metrics](monitoring.md)

Always know what’s going on with your deployment with
built-in [monitoring](https://github.com/PRANAY8055/aqueduct/blob/main/docs/assets/aqueduct-grafana-dashboard.json) of
throughput, response times, errors and more. This section contains guides, best practices and advises related to
managing Aqueduct in production.

## [Filters](filters.md)

Use the Filter feature provided in Aqueduct which allows you to apply any filters on the fields present in the key or
message of the event data set and helps you narrow it down to your use case-specific data.

## [Templating](templating.md)

Aqueduct provides various templating features

## [Decorators](decorators.md)

Decorators are used for chained processing of messages.

- SinkWithFailHandler
- SinkWithRetry
- SinkWithDlq
- SinkFinal

## [AqueductConsumer](consumer.md)

A aqueduct consumer read messages from kafka, pushes those messages to sink and commits offsets back to kafka based on certain strategies.

## [Offsets](offsets.md)

Offset manager is a data structure used to manage offsets asynchronously. An offset should only be committed when a
message is processed fully. Offset manager maintains a state of all the offsets of all topic-partitions, that can be
committed. It can also be used by sinks to manage its own offsets.
