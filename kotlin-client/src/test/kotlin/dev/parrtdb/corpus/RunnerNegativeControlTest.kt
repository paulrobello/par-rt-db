package dev.parrtdb.corpus

import java.io.File
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Proves the runner can fail: a corrupted expectation or a wrong error code must be caught. */
class RunnerNegativeControlTest {
    private fun mutated(caseName: String, from: String, to: String): File {
        val src = File(CorpusPaths.root, "semantics/$caseName.json").readText()
        assertTrue(src.contains(from), "$caseName no longer contains `$from`; update the control")
        val dir = createTempDirectory("kt-neg").toFile().also { it.deleteOnExit() }
        return File(dir, "$caseName.json").also {
            it.writeText(src.replace(from, to))
            it.deleteOnExit()
        }
    }

    @Test
    fun wrongExpectedValueFails() {
        val f = mutated("get-hit", "\"expect\"", "\"expect\": {\"definitely\": \"wrong\"}, \"_unused\"")
        assertFailsWith<SemanticsRunner.CaseFailure> { SemanticsRunner.run(f) }
    }

    @Test
    fun wrongErrorCodeFails() {
        val f = mutated("error-query-unknown-table", "NOT_FOUND", "CONFLICT")
        assertFailsWith<SemanticsRunner.CaseFailure> { SemanticsRunner.run(f) }
    }

    @Test
    fun kotlinSkipIsReportedNotHidden() {
        val f = mutated("get-hit", "\"expect\"", "\"skip\": {\"kotlin\": \"x\"}, \"expect\"")
        val outcome = SemanticsRunner.run(f)
        assertTrue(outcome is SemanticsRunner.Outcome.Skipped)
    }
}
