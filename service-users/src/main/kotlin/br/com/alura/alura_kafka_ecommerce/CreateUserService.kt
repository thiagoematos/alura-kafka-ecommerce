package br.com.alura.alura_kafka_ecommerce

import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID

fun main() {
    openConnection()
        .run {
            createTable()

            KafkaService(
                type = Order::class.java,
                topic = "ECOMMERCE_NEW_ORDER",
                groupId = "CreateUserService"
            ) { _, order ->
                order.email
                    .takeIf { isNewUser(searchKey = order.email) }
                    ?.let { insertNewUser(order.email) }
            }
        }.use(KafkaService<Order>::run)
}

private fun openConnection(): Connection =
    DriverManager.getConnection("jdbc:sqlite:users_database.db")

private fun Connection.createTable() =
    createStatement().execute("create table if not exists Users (uuid varchar(200) primary key, email varchar(200))")

private fun Connection.isNewUser(searchKey: String) =
    prepareStatement("select uuid from Users where email = ? limit 1")
        .apply { setString(1, searchKey) }
        .executeQuery()
        .next()
        .not()

private fun Connection.insertNewUser(email: String) =
    UUID.randomUUID().toString()
        .let { uuid ->
            prepareStatement("insert into Users (uuid, email) values (?, ?)")
                .apply {
                    setString(1, uuid)
                    setString(2, email)
                }
                .execute()
                .also { println("User($uuid, $email) added!") }
        }
