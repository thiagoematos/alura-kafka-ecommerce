package br.com.alura.alura_kafka_ecommerce

import java.util.concurrent.TimeUnit

fun main() =
    KafkaService(
        type = Order::class.java,
        topic = "ECOMMERCE_NEW_ORDER",
        groupId = "FraudDetectorService"
    ) { _, _ ->
        println("Processing new order, checking for fraud")
        TimeUnit.SECONDS.sleep(5)
        println("Order processed")
    }.use(KafkaService<Order>::run)
