package lgs.configuration

import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import lgs.l3.impl.ItemTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

@Factory
class DatabaseFactory {
    private val logger: Logger = LoggerFactory.getLogger(DatabaseFactory::class.java)
    private val driver = "org.sqlite.JDBC"

    @Singleton
    fun database(path: String?): Database {
        val url = "jdbc:sqlite:${path ?: "./database.db"}"

        logger.info("Connecting to url:$url with driver $driver")
        val db = Database.connect(url, driver)
        TransactionManager.manager.defaultIsolationLevel =
            Connection.TRANSACTION_SERIALIZABLE

        // Todo: move to a better alternative. Maybe flyway?
        logger.info("Creating tables")
        transaction (db) {
            SchemaUtils.create(ItemTable)
        }
        return db
    }
}
