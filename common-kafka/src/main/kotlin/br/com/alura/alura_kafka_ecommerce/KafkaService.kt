package br.com.alura.alura_kafka_ecommerce

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.io.Closeable
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.regex.Pattern

class KafkaService<T>
private constructor(
    private val type: Class<T>,
    private val groupId: String,
    overrideProperties: Map<String, Any>,
    private val parse: (String, T) -> Unit
) : Closeable {
    private val consumer = KafkaConsumer<String, T>(properties(overrideProperties))

    constructor(
        type: Class<T>,
        topic: String,
        groupId: String,
        overrideProperties: Map<String, Any> = emptyMap(),
        parse: (String, T) -> Unit = { _, _ -> }
    ) : this(type, groupId, overrideProperties, parse) {
        consumer.subscribe(listOf(topic))
    }

    constructor(
        type: Class<T>,
        topic: Pattern,
        groupId: String,
        overrideProperties: Map<String, Any> = emptyMap(),
        parse: (String, T) -> Unit = { _, _ -> }
    ) : this(type, groupId, overrideProperties, parse) {
        consumer.subscribe(topic)
    }

    fun run() {
        while (true) {
            consumer
                .poll(Duration.ofMillis(100))
                .takeUnless { it.isEmpty }
                ?.also { println("\n\nFounded ${it.count()} records") }
                ?.forEach {
                    it.topic().wrap {
                        it.logAbout()
                        parse(it.key(), it.value())
                    }
                }
        }
    }

    private fun String.wrap(body: () -> Unit) {
        println(this.surroundWithDashes())
        body()
        println(this.length.dashes().surroundWithDashes())
    }

    private fun String.surroundWithDashes() = 12.dashes() + this + 12.dashes()
    private fun Int.dashes() = "-".repeat(this)

    private fun ConsumerRecord<String, T>.logAbout() =
        println(
            """
        Key:       ${key()}
        Value:     ${value()}
        Partition: ${partition()}
        Offset:    ${offset()}
    """.trimIndent()
        )

    private fun properties(overrideProperties: Map<String, Any>) =
        Properties().apply {
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
            setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
            setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer::class.java.name)
            setProperty(GsonDeserializer.TYPE_CONFIG, type.name)
            setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())
            putAll(overrideProperties)
        }

    override fun close() = consumer.close()
}
