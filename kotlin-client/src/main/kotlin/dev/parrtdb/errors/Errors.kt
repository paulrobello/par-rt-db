package dev.parrtdb.errors

/**
 * Mirrors server/src/error.rs::ErrorCode and wire-corpus/error-codes.json.
 * The wire name is the enum `wire`; `httpStatus` is the canonical HTTP status.
 */
enum class ErrorCode(val wire: String, val httpStatus: Int) {
    UNAUTHORIZED("UNAUTHORIZED", 401),
    FORBIDDEN("FORBIDDEN", 403),
    NOT_FOUND("NOT_FOUND", 404),
    SCHEMA_VIOLATION("SCHEMA_VIOLATION", 422),
    PRECONDITION_FAILED("PRECONDITION_FAILED", 409),
    BAD_REQUEST("BAD_REQUEST", 400),
    INTERNAL("INTERNAL", 500),
    RATE_LIMITED("RATE_LIMITED", 429),
    CONFLICT("CONFLICT", 409),
    QUOTA_EXCEEDED("QUOTA_EXCEEDED", 507),
    UNSUPPORTED_PROTOCOL("UNSUPPORTED_PROTOCOL", 400),
    CURSOR_EXPIRED("CURSOR_EXPIRED", 410),
    READ_ONLY("READ_ONLY", 409);

    companion object {
        fun fromWire(code: String): ErrorCode? = entries.firstOrNull { it.wire == code }
    }
}

/** The `{code, message}` error envelope as a throwable. */
open class RtDbException(
    val code: ErrorCode,
    override val message: String,
    val retryAfter: Double? = null,
) : RuntimeException(message) {
    override fun toString(): String = "${code.wire}: $message"
}

fun badRequest(message: String): RtDbException = RtDbException(ErrorCode.BAD_REQUEST, message)
fun schemaViolation(message: String): RtDbException = RtDbException(ErrorCode.SCHEMA_VIOLATION, message)
fun notFound(message: String): RtDbException = RtDbException(ErrorCode.NOT_FOUND, message)
