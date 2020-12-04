import kotlinx.serialization.*
import kotlinx.serialization.json.*
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionType
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets

// This project works on top of the infrastructure set up in
// https://github.com/ypt/experiment-pulsar-connector Particularly, this project
// depends on the topic and data schema provided by the Pulsar Postres source
// connector set up there.
//
// Provided the CDC data piped into the
// "persistent://public/default/db.public.outbox" topic, this project will then
// use those messages to construct and route new messages to their intended
// destination topics.

private val json = Json { ignoreUnknownKeys = true }

@Serializable
private data class CdcEvent(
    val op: String,
    val after: OutboxAfterEvent
)

@Serializable
private data class OutboxAfterEvent(
    val aggregatetype: String,
    val aggregateid: String,
    val type: String,
    val payload: String,
    val mybytecol: String
)

private val logger = LoggerFactory.getLogger("main")

private val producers: MutableMap<String, Producer<ByteArray>> = mutableMapOf()

private fun getProducer(
    topic: String,
    producers: MutableMap<String, Producer<ByteArray>>,
    pulsarClient: PulsarClient
): Producer<ByteArray> {
    val producer = producers[topic]
    return if (producer != null) {
        producer
    } else {
        val newProducer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topic)
            .create()
        producers[topic] = newProducer
        return newProducer
    }
}

fun main(args: Array<String>) {
    val pulsarUrl = "pulsar://localhost:6650"
    val pulsarClient = PulsarClient
        .builder()
        .serviceUrl(pulsarUrl)
        .build()
    pulsarClient.newConsumer()
        // We only care about the outbox table and its corresponding topic. That
        // table and topic were set up in this other project:
        // https://github.com/ypt/experiment-pulsar-connector
        .topic("persistent://public/default/db.public.outbox")
        .subscriptionName("outboxRouter")
        .subscriptionType(SubscriptionType.Exclusive) // TODO: look into key-shared
        .messageListener { consumer, msg ->
            val message = String(msg.data, StandardCharsets.UTF_8)
            logger.debug("RECEIVED: {}", message)

            // Assuming the input data is from the Pulsar Postgres source connector,
            // the message will look something like the following JSON string
            // {
            //   "before": null,
            //   "after": {
            //     "id": "6c80c633-d37d-41e4-af08-bc20bbc9e0a7",
            //     "ts": "2020-12-03T20:55:05.154437Z",
            //     "aggregatetype": "myaggregatetype1",
            //     "aggregateid": "1",
            //     "type": "mytype",
            //     "payload": "{\"hello\": \"world 1\"}",
            //     "mybytecol": "AT19FtetT++2G9lbdlyM6w=="
            //   },
            // "source": {
            //     "version": "1.0.0.Final",
            //     "connector": "postgresql",
            //     "name": "db",
            //     "ts_ms": 1607028905155,
            //     "snapshot": "false",
            //     "db": "experiment",
            //     "schema": "public",
            //     "table": "outbox",
            //     "txId": 563,
            //     "lsn": 23523432,
            //     "xmin": null
            //   },
            //   "op": "c",
            //   "ts_ms": 1607028905164
            // }

            val cdcEvent = json.decodeFromString<CdcEvent>(message)

            // For our event outbox purposes, we only care about create
            // operations
            if (cdcEvent.op == "c") {
                // Route your message to a topic however you'd like
                val topic = "persistent://public/default/${cdcEvent.after.aggregatetype}"

                // Construct your payload however you'd like to. Below, the
                // payload column is a Postgres jsonb column, which the Pulsar
                // Postgres source connector will deliver as a string.
                // Alternatively, you could store binary data as a Postgres
                // bytea column, which the Pulsar Postgres source connector will
                // deliver as a base64 string, which we can then encode back
                // into a ByteArray
                val payload = cdcEvent.after.payload.encodeToByteArray()

                // send the message
                val producer = getProducer(topic, producers, pulsarClient)
                producer.send(payload)
            }

            // TODO: a failure right here could result in dupe sends. Take a
            //  closer look at transactions to counter the problem
            //  https://pulsar.apache.org/docs/en/transactions/

            consumer.acknowledge(msg)
        }
        .subscribe()
}
