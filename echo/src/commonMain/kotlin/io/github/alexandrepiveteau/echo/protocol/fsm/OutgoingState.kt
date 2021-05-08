@file:OptIn(
    InternalCoroutinesApi::class,
    EchoSyncPreview::class,
)
@file:Suppress("SameParameterValue")

package io.github.alexandrepiveteau.echo.protocol.fsm

import io.github.alexandrepiveteau.echo.EchoEventLogPreview
import io.github.alexandrepiveteau.echo.EchoSyncPreview
import io.github.alexandrepiveteau.echo.causal.SequenceNumber
import io.github.alexandrepiveteau.echo.causal.SiteIdentifier
import io.github.alexandrepiveteau.echo.logs.ImmutableEventLog
import io.github.alexandrepiveteau.echo.protocol.Message.Incoming as Inc
import io.github.alexandrepiveteau.echo.protocol.Message.Outgoing as Out
import io.github.alexandrepiveteau.echo.sync.SyncStrategy
import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.persistentListOf
import kotlinx.collections.immutable.plus
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.channels.receiveOrNull
import kotlinx.coroutines.selects.select

/**
 * A sealed class representing the different states that the finite state machine may be in. Each
 * step may decide to continue or stop processing with the [keepGoing] method, and defines state
 * transitions as part of the [step].
 *
 * @param T the type of the events.
 */
// TODO : Update the documentation.
internal sealed class OutgoingState<T, C> : State<Inc<T>, Out<T>, T, C, OutgoingState<T, C>> {

  companion object {

    /** Creates a new [OutgoingState] that's the beginning of the FSM. */
    operator fun <T, C> invoke(
        // TODO : Use the SyncStrategy
        strategy: SyncStrategy,
    ): OutgoingState<T, C> = OutgoingAdvertising(persistentListOf())
  }
}

/**
 * Indicates that a step with the given name should not be reachable.
 *
 * @param name the name of the unreachable step.
 */
private fun notReachable(name: String? = null): Throwable {
  return IllegalStateException("State ${name?.plus(" ")}should not be reachable")
}

// FINITE STATE MACHINE

private data class OutgoingAdvertising<T, C>(
    private val available: PersistentList<SiteIdentifier>,
) : OutgoingState<T, C>() {

  override suspend fun OutgoingStepScope<T, C>.step(
      log: ImmutableEventLog<T, C>,
  ): Effect<OutgoingState<T, C>> =
      when (val msg = receiveOrNull()) {
        is Inc.Advertisement -> {
          Effect.Move(
              copy(
                  available = available.plus(msg.site),
              ))
        }
        is Inc.Ready -> {
          Effect.Move(
              OutgoingListening(
                  pendingAcks = available,
                  pendingRequested = persistentListOf(),
                  requested = persistentListOf(),
              ))
        }
        null -> Effect.Terminate
        is Inc.Event -> Effect.MoveToError(notReachable())
      }
}

// TODO : Refactor this to Persistent states.
// TODO : Add more sophisticated precondition checks in protocol.
@OptIn(EchoEventLogPreview::class)
private data class OutgoingListening<T, C>(
    private val pendingAcks: PersistentList<SiteIdentifier>,
    private val pendingRequested: PersistentList<SiteIdentifier>,
    private val requested: PersistentList<SiteIdentifier>,
) : OutgoingState<T, C>() {

  override suspend fun OutgoingStepScope<T, C>.step(
      log: ImmutableEventLog<T, C>
  ): Effect<OutgoingState<T, C>> {
    val acknowledge = pendingAcks.lastOrNull()
    val expected = acknowledge?.let(log::expected) ?: SequenceNumber.Zero

    val request = pendingRequested.lastOrNull()

    return select {
      if (acknowledge != null) {
        onSend(
            Out.Acknowledge(
                site = acknowledge,
                nextSeqno = expected,
            )) {
          Effect.Move(
              copy(
                  pendingAcks = pendingAcks.removeAt(pendingAcks.lastIndex),
                  pendingRequested = pendingRequested.add(acknowledge),
              ))
        }
      }

      if (request != null) {
        onSend(
            Out.Request(
                site = request,
                count = UInt.MAX_VALUE,
            )) {
          Effect.Move(
              copy(
                  pendingRequested = pendingRequested.removeAt(pendingRequested.lastIndex),
                  requested = requested.add(request),
              ))
        }
      }

      onReceiveOrClosed { v ->
        when (val msg = v.valueOrNull) {
          null -> Effect.Terminate
          is Inc.Advertisement -> {
            Effect.Move(
                copy(
                    pendingAcks = pendingAcks.add(msg.site),
                ))
          }
          is Inc.Event -> {
            set(msg.seqno, msg.site, msg.body)
            Effect.Move(this@OutgoingListening)
          }
          is Inc.Ready -> Effect.MoveToError(notReachable())
        }
      }
    }
  }
}
