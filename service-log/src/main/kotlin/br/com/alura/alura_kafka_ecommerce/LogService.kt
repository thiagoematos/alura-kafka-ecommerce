package br.com.alura.alura_kafka_ecommerce

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.regex.Pattern

fun main() =
    KafkaService(
        type = String::class.java,
        topic = Pattern.compile("ECOMMERCE.*"),
        groupId = "LogService",
        overrideProperties = mapOf(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name
        )
    ).use(KafkaService<String>::run)
