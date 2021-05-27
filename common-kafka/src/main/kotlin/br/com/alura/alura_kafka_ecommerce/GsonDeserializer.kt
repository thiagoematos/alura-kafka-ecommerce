package br.com.alura.alura_kafka_ecommerce

import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Deserializer

class GsonDeserializer<T> : Deserializer<T> {
    private val gsonSerializer = GsonBuilder().create()
    private lateinit var type: Class<T>

    companion object {
        const val TYPE_CONFIG: String = "br.com.alura.alura_kafka_ecommerce.TYPE_CONFIG"
    }

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
        val clazzName = configs[TYPE_CONFIG]
            ?: throw RuntimeException("GsonDeserializer.TYPE_CONFIG not defined")

        type = Class.forName(clazzName.toString()) as Class<T>
    }

    override fun deserialize(topic: String?, data: ByteArray): T {
        return gsonSerializer.fromJson(String(data), type)
    }
}
