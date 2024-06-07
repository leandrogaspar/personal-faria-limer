package lgs.l3

import lgs.l3.model.Item

/**
 * L3 - Leandro's Lightweight Locker, AKA Poor Man's S3
 * Basically my own, dumb, riskier, and production unsafe blob storage.
 */
interface L3 {
    /**
     * Stores [content] under [key].
     * If it is the first time the [key] is being used, the created [Item] will have [Item.version] 1.
     * Future puts with the same [key] will create new [Item] wih incremented [Item.version].
     * @param [key] the unique key to store the content.
     * @param [content] to be stored.
     * @return the added [Item].
     */
    suspend fun putItem(key: String, content: ByteArray): Item

    /**
     * Retrieves [Item] under the specified [key] and [version], or null if none is found.
     * @param [key] the unique key for the desired item.
     * @param [version] the version to be retrieved. If none is provided, the latest [Item] will be retrieved.
     * @return the [Item].
     */
    suspend fun getItem(key: String, version: Int? = null): Item?

    /**
     * Soft deletes [Item] under the specified [key]. If there is no [Item] for the given [key], this is a no-op.
     * If the [key] is found, the [Item.deletedAt] timestamp is updated.
     * There is no hard delete in L3 and a deleted [Item] will still appear on [L3.getItem] calls.
     * @param [key] the unique key for the desired item.
     * @return the deleted [Item] or null if none was found.
     */
    suspend fun deleteItem(key: String): Item?
}
