package br.com.alura.alura_kafka_ecommerce

import java.math.BigDecimal

data class Order(
    private val orderId: String,
    private val amount: BigDecimal,
    private val email: String
)
