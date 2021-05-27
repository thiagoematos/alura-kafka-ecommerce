package br.com.alura.alura_kafka_ecommerce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.io.Closeable
import java.util.Properties

class KafkaDispatcher<T> : Closeable {
    private var producer = KafkaProducer<String, T>(properties())

    fun send(topic: String, key: String, value: T) {
        producer.send(ProducerRecord(topic, key, value), ::callback).get()
    }

    private fun callback(data: RecordMetadata, exception: Exception?) =
        exception
            ?.printStackTrace()
            ?: println(
                """Sending: ${data.topic()}
                |Partition: ${data.partition()}|Offset: ${data.offset()}|Timestamp: ${data.timestamp()}
                |Success!!""".trimMargin()
            )

    private fun properties() =
        Properties().apply {
            setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
            setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer::class.java.name)
        }

    override fun close() {
        producer.close()
    }
}