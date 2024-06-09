package lgs.publisher_consumer.sql_based

import org.jetbrains.exposed.sql.Table

object ConsumerTable : Table() {
    val group = varchar("consumer_group", 1000)
    val topic = varchar("topic", 1000)
    val messageId = uuid("message_id")
    val consumedAt = long("consumed_at")
    override val primaryKey = PrimaryKey(group, topic, messageId, name = "PK_Consumer")
}