package lgs.test_helpers

import java.io.File

fun cleanDbFile(dbFile: String) = File(dbFile).delete()
