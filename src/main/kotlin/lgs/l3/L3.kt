package lgs.l3

import lgs.l3.model.Item

/**
 * L3 - Leandro's Lightweight Locker, AKA Poor Man's S3
 * Basically my own, dumb, riskier, and production unsafe blob storage.
 */
interface L3 {
    /**
     * Stores [content] under [folder] / [key].
     * If it is the first time the [folder] / [key] is being used, the created [Item] will have [Item.version] 1.
     * Future puts with the same [folder] / [key] will create new [Item] wih incremented [Item.version].
     * @param [folder] the item folder. Folders serve to group items.
     * @param [key] the unique key to store the content.
     * @param [content] to be stored.
     * @return the added [Item].
     */
    suspend fun putItem(folder: String, key: String, content: ByteArray): Item

    /**
     * Retrieves [Item] under the specified [folder] / [key] and [version], or null if none is found.
     * @param [folder] the item folder.
     * @param [key] the unique key for the desired item.
     * @param [version] the version to be retrieved. If none is provided, the latest [Item] will be retrieved.
     * @return the [Item].
     */
    suspend fun getItem(folder: String, key: String, version: Int? = null): Item?

    /**
     * Soft deletes [Item] under the specified [folder] / [key].
     * If there is no [Item] for the given [folder] / [key], this is a no-op.
     * If the [key] is found, the [Item.deletedAt] timestamp is updated.
     * There is no hard delete in L3 and a deleted [Item] will still appear on [L3.getItem] calls.
     * @param [key] the unique key for the desired item.
     * @param [key] the unique key for the desired item.
     * @return the deleted [Item] or null if none was found.
     */
    suspend fun deleteItem(folder: String, key: String): Item?
}
