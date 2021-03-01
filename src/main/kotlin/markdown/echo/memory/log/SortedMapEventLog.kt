@file:OptIn(EchoPreview::class)

package markdown.echo.memory.log

import markdown.echo.EchoPreview
import markdown.echo.causal.EventIdentifier
import markdown.echo.causal.SequenceNumber
import markdown.echo.causal.SiteIdentifier
import java.util.*

/**
 * An implementation of a [MutableEventLog] that makes use of [java.util.SortedMap].
 *
 * @param T the type of the body of the events.
 */
internal class SortedMapEventLog<T> internal constructor(
    vararg events: Pair<EventIdentifier, T>,
) : MutableEventLog<T> {

    /**
     * The sorted structure that associates event identifiers to an operation body. Because event
     * identifiers are totally ordered, it's possible to efficient iterate on the events from the
     * [MutableEventLog].
     */
    private val buffer = mutableMapOf<SiteIdentifier, SortedMap<SequenceNumber, T>>()

    init {
        for ((key, value) in events) {
            this[key.seqno, key.site] = value
        }
    }

    override val sites: Set<SiteIdentifier>
        get() = buffer.keys

    override fun expected(
        site: SiteIdentifier,
    ) = buffer[site]?.lastKey()?.inc() ?: SequenceNumber.Zero

    override fun get(
        seqno: SequenceNumber,
        site: SiteIdentifier,
    ): T? = buffer[site]?.get(seqno)

    override fun events(
        seqno: SequenceNumber,
        site: SiteIdentifier,
    ): Iterable<Pair<EventIdentifier, T>> {
        // TODO : Change implementation so read-only events() does not mutate [buffer]
        return buffer.getOrPut(site) { sortedMapOf() }
            .tailMap(seqno)
            .asSequence()
            .map { (seqno, body) -> EventIdentifier(seqno, site) to body }
            .asIterable()
    }

    override operator fun set(
        seqno: SequenceNumber,
        site: SiteIdentifier,
        body: T,
    ) {
        buffer.getOrPut(site) { sortedMapOf() }[seqno] = body
    }
}