package io.github.alexandrepiveteau.echo

import io.github.alexandrepiveteau.echo.causal.SiteIdentifier
import io.github.alexandrepiveteau.echo.events.EventScope
import io.github.alexandrepiveteau.echo.internal.history.ActualPersistentHistory
import io.github.alexandrepiveteau.echo.internal.history.PersistentHistoryMutableSite
import io.github.alexandrepiveteau.echo.internal.history.PersistentHistorySite
import io.github.alexandrepiveteau.echo.logs.Change.Companion.skipped
import io.github.alexandrepiveteau.echo.logs.EventLog.IndexedEvent
import io.github.alexandrepiveteau.echo.logs.ImmutableEventLog
import io.github.alexandrepiveteau.echo.logs.PersistentEventLog
import io.github.alexandrepiveteau.echo.logs.persistentEventLogOf
import io.github.alexandrepiveteau.echo.projections.OneWayProjection
import io.github.alexandrepiveteau.echo.projections.TwoWayProjection
import io.github.alexandrepiveteau.echo.sync.SyncStrategy
import io.github.alexandrepiveteau.echo.protocol.Message.Incoming as Inc
import io.github.alexandrepiveteau.echo.protocol.Message.Outgoing as Out
import kotlinx.coroutines.flow.StateFlow

/**
 * An interface describing a [Site] in the distributed system.
 *
 * @param T the type of the events managed by this [Site].
 * @param M the type of the underlying aggregated model for this [Site].
 */
interface Site<T, out M> : Exchange<Inc<T>, Out<T>> {
  val value: StateFlow<M>
}

/**
 * A mutable version of [Site], which allows the insertion of the events [T] through its [event]
 * method. Each [MutableSite] is associated with a globally unique [SiteIdentifier], which will be
 * used when yielding events.
 *
 * @param T the type of the events managed by this [Site].
 * @param M the type of the underlying aggregated model for this [Site].
 */
interface MutableSite<T, out M> : Site<T, M> {

  /** The globally unique [SiteIdentifier] for this [Site]. */
  val identifier: SiteIdentifier

  /**
   * Creates some new events, that are generated in the [EventScope]. This function returns once the
   * events have been successfully added to the underlying [MutableSite].
   */
  suspend fun event(scope: suspend EventScope<T>.(M) -> Unit)
}

/**
 * Creates a new [Site], which can not be manually mutated. These sites are mostly used replication
 * purposes.
 *
 * @param log the underlying [PersistentEventLog] for this [site].
 * @param strategy the [SyncStrategy] for this site.
 * @param T the type of the events managed by this [Site].
 */
@OptIn(EchoSyncPreview::class)
fun <T> site(
    log: PersistentEventLog<T, Nothing> = persistentEventLogOf(),
    strategy: SyncStrategy = SyncStrategy.Continuous,
): Site<T, ImmutableEventLog<T, Nothing>> =
    unorderedSite(
        initial = persistentEventLogOf(),
        log = log,
        strategy = strategy,
    ) { entry, model ->
      model.apply {
        set(entry.identifier.site, entry.identifier.seqno, entry.body, change = skipped())
      }
    }

/**
 * Creates a new [MutableSite] for the provided [SiteIdentifier], with a backing [log].
 *
 * @param identifier the globally unique identifier for this [Site].
 * @param log the underlying [io.github.alexandrepiveteau.echo.logs.PersistentEventLog] for this
 * [MutableSite].
 * @param strategy the [SyncStrategy] for this site.
 *
 * @param T the type of the events managed by this [Site].
 */
@OptIn(EchoSyncPreview::class)
fun <T> mutableSite(
    identifier: SiteIdentifier,
    log: PersistentEventLog<T, Nothing> = persistentEventLogOf(),
    strategy: SyncStrategy = SyncStrategy.Continuous,
): MutableSite<T, PersistentEventLog<T, Nothing>> =
    unorderedMutableSite(
        identifier = identifier,
        initial = persistentEventLogOf(),
        log = log,
        strategy = strategy,
    ) { entry, model ->
      model.apply {
        set(entry.identifier.site, entry.identifier.seqno, entry.body, change = skipped())
      }
    }

/**
 * Creates a new [MutableSite] for the provided [SiteIdentifier], with a backing [log].
 * Additionally, this overload takes a [OneWayProjection] and lets you specify a projection to apply
 * to the data, to have custom [MutableSite.event] arguments.
 *
 * @param identifier the globally unique identifier for this [Site].
 * @param log the underlying [PersistentEventLog] for this [MutableSite].
 * @param initial the initial value for the projection aggregate.
 * @param strategy the [SyncStrategy] for this site.
 * @param projection the [OneWayProjection] for this [Site].
 *
 * @param M the type of the model for this [Site].
 * @param T the type of the events managed by this [Site].
 */
@OptIn(EchoSyncPreview::class)
fun <M, T> mutableSite(
    identifier: SiteIdentifier,
    initial: M,
    log: PersistentEventLog<T, M> = persistentEventLogOf(),
    strategy: SyncStrategy = SyncStrategy.Continuous,
    projection: OneWayProjection<M, IndexedEvent<T>>,
): MutableSite<T, M> =
    unorderedMutableSite(
        identifier = identifier,
        initial = initial,
        log = log,
        strategy = strategy,
        projection = projection,
    )

/**
 * Creates a new [MutableSite] for the provided [SiteIdentifier], with a backing [log].
 * Additionally, this overload takes a [TwoWayProjection] and lets you specify a projection to apply
 * to the data, to have custom [MutableSite.event] arguments.
 *
 * @param identifier the globally unique identifier for this [Site].
 * @param log the underlying [PersistentEventLog] for this [MutableSite].
 * @param initial the initial value for the projection aggregate.
 * @param strategy the [SyncStrategy] for this site.
 * @param projection the [TwoWayProjection] for this [Site].
 *
 * @param M the type of the model for this [Site].
 * @param T the type of the events managed by this [Site].
 * @param C the type of the changes generated by this [Site].
 */
@OptIn(EchoSyncPreview::class)
fun <M, T, C> mutableSite(
    identifier: SiteIdentifier,
    initial: M,
    log: PersistentEventLog<T, C> = persistentEventLogOf(),
    strategy: SyncStrategy = SyncStrategy.Continuous,
    projection: TwoWayProjection<M, IndexedEvent<T>, C>,
): MutableSite<T, M> =
    unorderedMutableSite(
        identifier = identifier,
        initial = initial,
        log = log,
        strategy = strategy,
        projection = projection,
    )

// SITE BUILDERS

@EchoSyncPreview
internal fun <M, T> unorderedSite(
    initial: M,
    log: PersistentEventLog<T, M> = persistentEventLogOf(),
    strategy: SyncStrategy,
    projection: OneWayProjection<M, IndexedEvent<T>>,
): Site<T, M> =
    PersistentHistorySite(
        initial = ActualPersistentHistory(initial, log, projection),
        strategy = strategy,
    )

// MUTABLE SITE BUILDERS

@EchoSyncPreview
internal fun <M, T> unorderedMutableSite(
    identifier: SiteIdentifier,
    initial: M,
    log: PersistentEventLog<T, M> = persistentEventLogOf(),
    strategy: SyncStrategy,
    projection: OneWayProjection<M, IndexedEvent<T>>,
): MutableSite<T, M> =
    PersistentHistoryMutableSite(
        identifier = identifier,
        initial = ActualPersistentHistory(initial, log, projection),
        strategy = strategy,
    )

@EchoSyncPreview
internal fun <M, T, C> unorderedMutableSite(
    identifier: SiteIdentifier,
    initial: M,
    log: PersistentEventLog<T, C> = persistentEventLogOf(),
    strategy: SyncStrategy,
    projection: TwoWayProjection<M, IndexedEvent<T>, C>,
): MutableSite<T, M> =
    PersistentHistoryMutableSite(
        identifier = identifier,
        initial = ActualPersistentHistory(initial, log, projection),
        strategy = strategy,
    )
