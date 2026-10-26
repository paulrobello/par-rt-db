package dev.parrtdb.http

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.suspendCancellableCoroutine

/** Suspends until the future completes; cancelling the coroutine cancels the future. */
internal suspend fun <T> CompletableFuture<T>.await(): T =
    suspendCancellableCoroutine { cont ->
        whenComplete { value, error ->
            if (error == null) cont.resume(value) else cont.resumeWithException((error as? CompletionException)?.cause ?: error)
        }
        cont.invokeOnCancellation { cancel(true) }
    }
