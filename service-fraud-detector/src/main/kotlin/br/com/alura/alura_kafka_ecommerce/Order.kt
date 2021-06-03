package br.com.alura.alura_kafka_ecommerce

import java.math.BigDecimal

data class Order(
    private val orderId: String,
    internal val amount: BigDecimal,
    internal val email: String
)
