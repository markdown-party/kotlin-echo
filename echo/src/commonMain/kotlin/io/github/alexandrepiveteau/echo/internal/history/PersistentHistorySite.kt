package io.github.alexandrepiveteau.echo.internal.history

import io.github.alexandrepiveteau.echo.EchoEventLogPreview
import io.github.alexandrepiveteau.echo.MutableSite
import io.github.alexandrepiveteau.echo.causal.EventIdentifier
import io.github.alexandrepiveteau.echo.causal.SequenceNumber
import io.github.alexandrepiveteau.echo.causal.SiteIdentifier
import io.github.alexandrepiveteau.echo.channelLink
import io.github.alexandrepiveteau.echo.events.EventScope
import io.github.alexandrepiveteau.echo.logs.EventLog
import io.github.alexandrepiveteau.echo.logs.PersistentEventLog
import io.github.alexandrepiveteau.echo.protocol.fsm.Effect
import io.github.alexandrepiveteau.echo.protocol.fsm.IncomingState
import io.github.alexandrepiveteau.echo.protocol.fsm.OutgoingState
import io.github.alexandrepiveteau.echo.protocol.fsm.State
import kotlinx.coroutines.flow.*

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
 * An implementation of [MutableSite] that wraps a [PersistentLogHistory].
 *
 * @param T the type of the events.
 * @param M the type of the model.
 * @param C the type of the changes.
 */
internal class PersistentHistorySite<T, M, C>(
    override val identifier: SiteIdentifier,
    initial: PersistentLogHistory<T, M, C>,
) : MutableSite<T, M> {

  /** The current value of in the [PersistentHistory]. */
  private val current = MutableStateFlow(initial)

  /** Mutates the [current] value atomically, using the function [f]. */
  private inline fun mutate(
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
  private inline fun <R> mutate(
      extract: (R) -> PersistentLogHistory<T, M, C>,
      f: (history: PersistentLogHistory<T, M, C>) -> R,
  ): R {
    while (true) {
      // Compare-and-swap mutation.
      val start = current.value
      val next = f(start)
      if (current.compareAndSet(start, extract(next))) return next
    }
  }

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
  private fun <I, O, S : State<I, O, T, S>> exchange(
      initial: suspend () -> S,
  ) =
      channelLink<I, O> { inc ->
        var state = initial()
        val insertions = current.map { it.current.log }.distinctUntilChanged().produceIn(this)

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
  override val value: Flow<M> = current.map { it.current.model }.distinctUntilChanged()

  override fun outgoing() = exchange { OutgoingState() }
  override fun incoming() = exchange { IncomingState(current.value.current.log.sites) }

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
