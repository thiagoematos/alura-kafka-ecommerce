package br.com.alura.alura_kafka_ecommerce

import java.util.concurrent.TimeUnit.SECONDS

fun main() =
    KafkaService(
        type = String::class.java,
        topic = "ECOMMERCE_SEND_EMAIL",
        groupId = "EmailService",
        overrideProperties = emptyMap()
    ) { _, _ ->
        println("Sending email")
        SECONDS.sleep(1)
        println("Email sent")
    }.use(KafkaService<String>::run)
