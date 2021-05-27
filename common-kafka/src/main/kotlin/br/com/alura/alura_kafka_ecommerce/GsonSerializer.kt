package br.com.alura.alura_kafka_ecommerce

import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Serializer

class GsonSerializer<T> : Serializer<T> {

    private val gsonSerializer = GsonBuilder().create()

    override fun serialize(topic: String, data: T) = gsonSerializer.toJson(data).toByteArray()
}
