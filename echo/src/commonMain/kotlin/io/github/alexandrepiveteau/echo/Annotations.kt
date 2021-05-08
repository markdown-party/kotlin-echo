package io.github.alexandrepiveteau.echo

import kotlin.RequiresOptIn.Level.ERROR

/**
 * This EventLog API is still experimental, and will likely be reworked and refactored. Please make
 * sure you understand the tradeoffs before using it.
 */
@RequiresOptIn(
    level = ERROR,
    message =
        """
        This EventLog API is still experimental, and will likely be reworked and refactored. Please
        make sure you understand the tradeoffs before using it.
        """,
)
@MustBeDocumented
annotation class EchoEventLogPreview

/**
 * Annotation for experimental sync APIs, which may change drastically as the replication protocol
 * gains more capabilities.
 */
@RequiresOptIn(
    level = ERROR,
    message = "This sync API is experimental and its design may change in the future.",
)
@MustBeDocumented
annotation class EchoSyncPreview
