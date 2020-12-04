# experiment-event-outbox-router

A experiment with [transactional event
oubox](https://microservices.io/patterns/data/transactional-outbox.html) message
routing based on data from [Pulsar's built-in Debezium Postgres source
connector](https://pulsar.apache.org/docs/en/io-connectors/#debezium-postgresql).

This project works on top of the infrastructure set up in
[experiment-pulsar-connector](https://github.com/ypt/experiment-pulsar-connector).
Particularly, this project depends on the topic and data schema provided by the
Pulsar Postres source connector set up there. Provided the CDC data piped into
the `db.public.outbox` topic, this project will then use those messages to
construct and route new messages to their intended destination topics.

![System
diagram](/docs/system-diagram.svg?raw=true&sanitize=true
"System diagram")

## Why?
Why go through all this trouble to set up such a system? Why not simply let your
application direcly write to both its database and Pulsar? In short, dual writes
are suceptible to 1) race conditions and 2) partial failures. For a good
explanation, see Martin Kleppmann's talk ["Using logs to build a solid data
infrastructure (or: why dual writes are a bad
idea)"](https://www.confluent.io/blog/using-logs-to-build-a-solid-data-infrastructure-or-why-dual-writes-are-a-bad-idea/).

## Run it

First, start up the following via the instructions from
[experiment-pulsar-connector](https://github.com/ypt/experiment-pulsar-connector).

1. Pulsar
1. Postgres
1. Pulsar Postgres source connector

Then start the router
```sh
./gradlew run
```

Connect a Pulsar consumer to `persistent://public/default/myaggregatetype1`
```sh
docker exec -it experiment-pulsar-connector_pulsar_1 /pulsar/bin/pulsar-client consume -s "mysubscription" persistent://public/default/myaggregatetype1 -n 0
```

Now let's insert some data into Postgres that will be routed to the above topic
```sh
docker exec -it experiment-pulsar-connector_db_1 psql experiment experiment

INSERT INTO outbox (aggregatetype, aggregateid, type, payload, mybytecol) VALUES('myaggregatetype1', 1, 'mytype', '{"hello":"world 1"}', decode('013d7d16d7ad4fefb61bd95b765c8ceb', 'hex'));
```

A message should appear in the `persistent://public/default/myaggregatetype1`
topic.

Here are a few other things you can try:

1. Currently, the routing logic is set to use the value in the `aggregatetype`
   column as the destination topic. You can try inserting more data with
   different `aggregatetype` values and watch them go to those different topics.
1. How does the timing and order of transaction `COMMIT`s relate to when the CDC
   data is sent?
1. What happens with multiple `INSERT`s in a single transaction?
1. What happens if you `ROLLBACK` a transaction?
1. What happens if `experiment-event-outbox-router` is offline while data is
   being inserted into Postgres and then brought back up afterwards?
1. What happens if Pulsar is offline while data is being inserted into Postgres
   and then brought back up afterwards?