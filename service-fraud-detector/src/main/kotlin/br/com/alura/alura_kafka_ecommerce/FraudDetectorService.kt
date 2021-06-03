package br.com.alura.alura_kafka_ecommerce

import java.math.BigDecimal
import java.util.concurrent.TimeUnit

private val orderDispatcher = KafkaDispatcher<Order>()

private data class Result(val message: String, val topic: String)

fun main() =
    KafkaService(
        type = Order::class.java,
        topic = "ECOMMERCE_NEW_ORDER",
        groupId = "FraudDetectorService"
    ) { _, order ->
        println("Processing new order, checking for fraud")
        TimeUnit.SECONDS.sleep(5)

        val results = mapOf(
            true to Result("It is a fraud", "ECOMMERCE_ORDER_REJECT"),
            false to Result("Order approved", "ECOMMERCE_ORDER_APPROVED")
        )

        results.basedOn(order)
            .let { result ->
                println("${result.message}: $order")
                orderDispatcher.send(result.topic, order.email, order)
            }

        println("Order processed")
    }.use(KafkaService<Order>::run)

private fun Map<Boolean, Result>.basedOn(order: Order) = this[order.isFraud()]!!
private fun Order.isFraud() = this.amount > BigDecimal("2500")
