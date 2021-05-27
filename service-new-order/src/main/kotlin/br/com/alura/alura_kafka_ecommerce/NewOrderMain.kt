package br.com.alura.alura_kafka_ecommerce

import java.math.BigDecimal
import java.util.UUID

fun main() {
    KafkaDispatcher<Order>().use { orderDispatcher ->
        KafkaDispatcher<String>().use { emailDispatcher ->
            for (i in 1..10) {
                val userId = UUID.randomUUID().toString()

                orderDispatcher.send(
                    topic = "ECOMMERCE_NEW_ORDER",
                    key = userId,
                    value = Order(
                        userId,
                        orderId = UUID.randomUUID().toString(),
                        amount = BigDecimal(Math.random() * 5000 + 1)
                    )
                )
                emailDispatcher.send(
                    topic = "ECOMMERCE_SEND_EMAIL",
                    key = userId,
                    value = "Thank you for your order! We are processing your order!"
                )
            }
        }
    }
}
