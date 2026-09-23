# elasticsearch-quartz

A Quartz `JobStore` implementation backed by Elasticsearch, designed for clustered Spring Boot applications that need persistent, distributed job scheduling without a relational database.

## Overview

`ElasticsearchJobStore` stores Quartz jobs and triggers as documents in an Elasticsearch index. It supports clustered deployments out of the box — multiple scheduler nodes share the same index and coordinate trigger acquisition via Elasticsearch's optimistic concurrency control.

Supported trigger types:
- `SimpleTriggerImpl`
- `CronTriggerImpl`

## Requirements

- Java 11+
- Spring Boot 2.6+
- Elasticsearch 8.x
- Quartz 2.3.x

## Configuration

Add the following to your `application.properties` or `application.yml`:

```properties
quartz.enabled=true

elastic.server-url=localhost
elastic.port=9200
elastic.scheme=https
elastic.user=elastic
elastic.password=changeme
```

Optional proxy support:

```properties
elastic.proxy=http://proxy-host:3128
elastic.server-path-prefix=/optional-prefix
```

The scheduler automatically creates an Elasticsearch index named `quartz` on startup if it does not already exist.

## Usage

The `ElasticsearchClient` and `SchedulerFactoryBean` beans are auto-configured. Use the Quartz `Scheduler` bean as you normally would:

```java
@Autowired
Scheduler scheduler;

scheduler.scheduleJob(jobDetail, trigger);
```

Jobs are Spring beans — `@Autowired` dependencies are injected automatically via `SpringBeanJobFactory`.

## Index structure

Each job and trigger is stored as a separate document. Triggers carry the following fields: `name`, `group`, `state`, `nextFireTime`, `previousFireTime`, `executionTimeout`, `maxExecutionTime`, `cronExpression`, `repeatCount`, `repeatInterval`, and `jobName`/`jobGroup` for the job association.

## Trigger timeout

Set a `timeout` key (milliseconds) in the job's `JobDataMap` to bound how long a trigger may remain in the `ACQUIRED` state before it is re-queued:

```java
jobDataMap.put("timeout", 30_000); // 30 seconds
```

If omitted, the default is 1 hour. Set to `0` to disable the timeout.
