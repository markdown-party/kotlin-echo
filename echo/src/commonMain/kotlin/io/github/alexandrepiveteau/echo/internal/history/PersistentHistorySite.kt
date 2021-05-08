package io.github.alexandrepiveteau.echo.internal.history

import io.github.alexandrepiveteau.echo.*
import io.github.alexandrepiveteau.echo.causal.EventIdentifier
import io.github.alexandrepiveteau.echo.causal.SequenceNumber
import io.github.alexandrepiveteau.echo.causal.SiteIdentifier
import io.github.alexandrepiveteau.echo.events.EventScope
import io.github.alexandrepiveteau.echo.internal.flow.map
import io.github.alexandrepiveteau.echo.logs.EventLog
import io.github.alexandrepiveteau.echo.logs.PersistentEventLog
import io.github.alexandrepiveteau.echo.protocol.fsm.Effect
import io.github.alexandrepiveteau.echo.protocol.fsm.IncomingState
import io.github.alexandrepiveteau.echo.protocol.fsm.OutgoingState
import io.github.alexandrepiveteau.echo.protocol.fsm.State
import io.github.alexandrepiveteau.echo.sync.SyncStrategy
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.yield

internal data class HistoryModel<T, M, C>(
    val log: PersistentEventLog<T, C>,
    val model: M,
)

internal data class HistoryEvent<T>(
    val site: SiteIdentifier,
    val seqno: SequenceNumber,
    override val body: T,
) : EventLog.IndexedEvent<T> {
  override val identifier = EventIdentifier(seqno, site)
}

// A typealias for PersistentHistory which contains a LogModel.
internal typealias PersistentLogHistory<T, M, C> =
    PersistentHistory<HistoryEvent<T>, HistoryModel<T, M, C>, EventIdentifier>

/**
 * An implementation of [Site] that wraps a [PersistentLogHistory].
 *
 * @param initial the base case of the model.
 * @param strategy the [SyncStrategy] to use for this [Site].
 *
 * @param T the type of the events.
 * @param M the type of the model.
 * @param C the type of the changes.
 */
@EchoSyncPreview
internal open class PersistentHistorySite<T, M, C>(
    initial: PersistentLogHistory<T, M, C>,
    private val strategy: SyncStrategy,
) : Site<T, M> {

  /** The current value of in the [PersistentHistory]. */
  internal val current = MutableStateFlow(initial)

  /** The [Mutex] that protects atomic mutations of the [current] [MutableStateFlow]. */
  private val mutex = Mutex()

  /** Mutates the [current] value atomically, using the function [f]. */
  private suspend inline fun mutate(
      f: (history: PersistentLogHistory<T, M, C>) -> PersistentLogHistory<T, M, C>
  ) {
    mutate(extract = { it }, f)
  }

  /**
   * Mutates the [current] value atomically, using the function [f].
   *
   * @param R the type of the return value of a mutation.
   *
   * @return the resulting data.
   */
  internal suspend inline fun <R> mutate(
      extract: (R) -> PersistentLogHistory<T, M, C>,
      f: (history: PersistentLogHistory<T, M, C>) -> R,
  ): R = mutex.withLock { f(current.value).apply { current.value = extract(this) } }

  /**
   * Runs an exchange based on a final state machine. The FSM starts with an [initial] state, and
   * performs steps as messages are received or sent.
   *
   * @param initial a lambda that creates the initial FSM state.
   *
   * @param I the type of the input messages.
   * @param O the type of the output messages.
   * @param S the type of the FSM states.
   */
  private fun <I, O, S : State<I, O, T, C, S>> exchange(initial: S) =
      channelLink<I, O> { inc ->
        var state = initial
        val insertions = current.map { it.current.log }.produceIn(this)

        // Give the producer a chance to issue closed events.
        yield()

        // Prepare some context information for the step.
        val scope =
            StepScopeImpl(inc, this, insertions) { seqno, site, body: T ->
              mutate { log -> log.forward(HistoryEvent(site, seqno, body)).first }
            }

        while (true) {
          // Run the effects, until we've moved to an error state or we were explicitly asked to
          // terminate.
          when (val effect = with(state) { scope.step(current.value.current.log) }) {
            is Effect.Move -> state = effect.next
            is Effect.MoveToError -> throw effect.problem
            is Effect.Terminate -> break
          }
        }

        // Clear the jobs registered in channelLink.
        inc.cancel()
        insertions.cancel()
      }

  // The current model value flow.
  override val value: StateFlow<M> = current.map { it.current.model }

  override fun outgoing() = exchange(OutgoingState(strategy))
  override fun incoming() = exchange(IncomingState(strategy))
}

/**
 * An implementation of [MutableSite] that wraps a [PersistentLogHistory].
 *
 * @param identifier the [SiteIdentifier] used to yield events.
 * @param initial the base case of the model.
 * @param strategy the [SyncStrategy] to use for this [Site].
 *
 * @param T the type of the events.
 * @param M the type of the model.
 * @param C the type of the changes.
 */
@EchoSyncPreview
internal open class PersistentHistoryMutableSite<T, M, C>(
    override val identifier: SiteIdentifier,
    initial: PersistentLogHistory<T, M, C>,
    strategy: SyncStrategy,
) : PersistentHistorySite<T, M, C>(initial, strategy), MutableSite<T, M> {

  @OptIn(EchoEventLogPreview::class)
  override suspend fun event(
      scope: suspend EventScope<T>.(M) -> Unit,
  ) {

    // TODO : fun interface when b/KT-40165 is fixed.
    val impl =
        object : EventScope<T> {
          override suspend fun yield(event: T) =
              mutate(extract = { it.first }) { history ->
                    history.forward(
                        HistoryEvent(
                            site = identifier,
                            seqno = history.current.log.expected,
                            body = event,
                        ),
                    )
                  }
                  .second
        }

    // Invoke on the current model.
    scope(impl, current.value.current.model)
  }
}
