package lgs.configuration

import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import lgs.l3.sql_based.ItemTable
import lgs.machado.sql_based.ConsumerTable
import lgs.machado.sql_based.MessageTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.DatabaseConfig
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import org.jetbrains.exposed.sql.transactions.experimental.withSuspendTransaction
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection
import kotlin.coroutines.CoroutineContext

@Factory
class DatabaseFactory {
    private val logger: Logger = LoggerFactory.getLogger(DatabaseFactory::class.java)
    private val driver = "org.sqlite.JDBC"

    @Singleton
    fun database(path: String?): Database {
        val url = "jdbc:sqlite:${path ?: "./dbs/database.db?journal_mode=WAL"}"

        logger.info("Connecting to url:$url with driver $driver")
        val db = Database.connect(
            url = url,
            driver = driver,
            databaseConfig = DatabaseConfig {
                useNestedTransactions = false
                defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
            }
        )

        // Todo: move to a better alternative. Maybe flyway?
        logger.info("Creating tables")
        transaction(db) {
            SchemaUtils.create(
                ItemTable,
                MessageTable,
                ConsumerTable,
            )
        }
        return db
    }
}

suspend fun <T> suspendedTransaction(
    context: CoroutineContext? = null,
    db: Database? = null,
    transactionIsolation: Int? = null,
    statement: suspend Transaction.() -> T
): T {
    val existing = TransactionManager.currentOrNull()
    if (existing == null) {
        // Create a new transaction, it will propagate the actual tx reference
        // using coroutine ThreadContextElements into the TransactionManager if this method is nested
        return newSuspendedTransaction(
            context = context,
            db = db,
            transactionIsolation = transactionIsolation,
            statement = statement
        )
    } else {
        return existing.withSuspendTransaction(context = context, statement = statement)
    }
}
