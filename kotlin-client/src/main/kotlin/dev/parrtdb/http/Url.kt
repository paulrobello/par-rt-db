package dev.parrtdb.http

private const val HEX = "0123456789ABCDEF"

private fun escape(s: String, keep: String, spaceAsPlus: Boolean): String {
    val sb = StringBuilder()
    for (b in s.toByteArray(Charsets.UTF_8)) {
        val c = (b.toInt() and 0xff).toChar()
        when {
            c in 'a'..'z' || c in 'A'..'Z' || c in '0'..'9' || keep.indexOf(c) >= 0 -> sb.append(c)
            c == ' ' && spaceAsPlus -> sb.append('+')
            else -> sb.append('%').append(HEX[(b.toInt() shr 4) and 0xf]).append(HEX[b.toInt() and 0xf])
        }
    }
    return sb.toString()
}

/** Go `url.PathEscape`: escapes one path segment (`/`, `;`, `,`, `?` and non-ASCII included). */
internal fun pathEscape(s: String): String = escape(s, "-_.~$&+=:@", false)

/** Go `url.QueryEscape`. */
internal fun queryEscape(s: String): String = escape(s, "-_.~", true)

/** Go `url.Values.Encode`: keys sorted, values kept in insertion order per key. */
internal fun queryString(params: List<Pair<String, String>>): String =
    params.sortedBy { it.first }.joinToString("&") { "${queryEscape(it.first)}=${queryEscape(it.second)}" }

/** `path` or `path?query` when [params] is non-empty. */
internal fun withQuery(path: String, params: List<Pair<String, String>>): String =
    if (params.isEmpty()) path else path + "?" + queryString(params)
