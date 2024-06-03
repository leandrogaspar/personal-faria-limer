package lgs.machado.sql_based

import org.jetbrains.exposed.sql.Table

object GroupConsumeTable: Table() {
    val consumerGroup = varchar("consumer_group", 1000)
    val topic = varchar("topic", 1000)
    val messageId = uuid("message_id")
    val consumedAt = long("consumed_at")
    override val primaryKey = PrimaryKey(consumerGroup, topic, messageId, name = "PK_GroupConsume")
}