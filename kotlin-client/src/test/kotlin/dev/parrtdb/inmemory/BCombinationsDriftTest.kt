package dev.parrtdb.inmemory

import java.io.File
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertTrue

class BCombinationsDriftTest {
    @Test
    fun embeddedRulesMatchWireCorpus() {
        val projectDir = File(System.getProperty("user.dir"))
        val original = File(projectDir.parentFile, "wire-corpus/query-combinations.json")
        assertTrue(original.exists(), "missing ${original.path}")
        val embedded =
            BCombinationsDriftTest::class.java.classLoader.getResourceAsStream("query-combinations.json")!!.use { it.readBytes() }
        assertContentEquals(original.readBytes(), embedded)
    }
}
