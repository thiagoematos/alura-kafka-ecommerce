package br.com.alura.alura_kafka_ecommerce

import java.math.BigDecimal

data class Order(
    internal val userId: String,
    private val orderId: String,
    private val amount: BigDecimal,
    internal val email: String
)
