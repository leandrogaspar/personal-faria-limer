package lgs.configuration

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import lgs.object_storage.sql_based.ItemTable
import lgs.publisher_consumer.sql_based.ConsumerTable
import lgs.publisher_consumer.sql_based.MessageTable
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
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.seconds

data class Databases(val writer: Database, val reader: Database)

@Factory
class DatabaseFactory {
    private val logger: Logger = LoggerFactory.getLogger(DatabaseFactory::class.java)
    private val driver = "org.sqlite.JDBC"

    @Singleton
    fun database() = Databases(
        writer = createDatabase(readOnly = false),
        reader = createDatabase(readOnly = false),
    )

    fun createDatabase(
        path: String? = "./dbs/database.db",
        readOnly: Boolean = false,
    ): Database {
        val config = HikariConfig().apply {
            jdbcUrl = "jdbc:sqlite:${path}?journal_mode=WAL"
            driverClassName = driver
            maximumPoolSize = if (readOnly) 10 else 1
            isReadOnly = readOnly
            transactionIsolation = "TRANSACTION_SERIALIZABLE"
            connectionTimeout = 2.seconds.inWholeMilliseconds
        }
        val dataSource = HikariDataSource(config)
        val db = Database.connect(
            datasource = dataSource,
            databaseConfig = DatabaseConfig {
                useNestedTransactions = false
            },
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
