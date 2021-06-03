package br.com.alura.alura_kafka_ecommerce

import spark.Spark.get
import java.math.BigDecimal
import java.util.UUID

private val orderDispatcher = KafkaDispatcher<Order>()
private val emailDispatcher = KafkaDispatcher<String>()

fun main() {
    get("/new") { req, res ->
        val email = req.queryParams("email")
        val amount = req.queryParams("amount")

        orderDispatcher.send(
            topic = "ECOMMERCE_NEW_ORDER",
            key = email,
            value = Order(
                orderId = UUID.randomUUID().toString(),
                amount = BigDecimal(amount),
                email = email
            )
        )
        emailDispatcher.send(
            topic = "ECOMMERCE_SEND_EMAIL",
            key = email,
            value = "Thank you for your order! We are processing your order!"
        )

        res.status(200)

        "Order sent!"
    }
}
