@file:OptIn(
    InternalCoroutinesApi::class,
    EchoSyncPreview::class,
)
@file:Suppress("SameParameterValue")

package io.github.alexandrepiveteau.echo.protocol.fsm

import io.github.alexandrepiveteau.echo.EchoSyncPreview
import io.github.alexandrepiveteau.echo.causal.EventIdentifier
import io.github.alexandrepiveteau.echo.causal.SequenceNumber
import io.github.alexandrepiveteau.echo.causal.SequenceNumber.Companion.Zero
import io.github.alexandrepiveteau.echo.causal.SiteIdentifier
import io.github.alexandrepiveteau.echo.logs.ImmutableEventLog
import io.github.alexandrepiveteau.echo.protocol.Message.Incoming as Inc
import io.github.alexandrepiveteau.echo.protocol.Message.Outgoing as Out
import io.github.alexandrepiveteau.echo.protocol.fsm.Effect.Move
import io.github.alexandrepiveteau.echo.protocol.fsm.Effect.Terminate
import io.github.alexandrepiveteau.echo.sync.SyncStrategy
import io.github.alexandrepiveteau.echo.util.plusBoundOverflows
import kotlinx.collections.immutable.*
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.selects.select

/**
 * An [IncomingState] represents one side of the replication protocol state machine. Each subclass
 * is a different state, which moves step by step and devices whether to continue or finish.
 *
 * @param T the type of the events.
 * @param C the type of the changes.
 */
internal sealed class IncomingState<T, C> : State<Out<T>, Inc<T>, T, C, IncomingState<T, C>> {

  companion object {

    /** Creates a new [IncomingState] that's the beginning of the FSM. */
    operator fun <T, C> invoke(
        strategy: SyncStrategy,
    ): IncomingState<T, C> =
        IncomingNew(
            strategy = strategy,
            alreadySent = persistentListOf(),
        )
  }
}

// FINITE STATE MACHINE

/**
 * The state when we are still advertising the sites and have not started issuing any events. This
 * can be seen as a sort of handshake phase :
 *
 * 1. Advertisements and sequence numbers are sent for all the sites that we know of.
 * 2. A Ready message is sent.
 * 3. Finally, we can send some events, or some additional advertisements.
 *
 * This allows for some optimizations and complex behaviors, in particular one-shot sync. One-shot
 * sync, for instance, means that the current site will only issue events from sites that were
 * advertised before the Ready state was reached, and will terminate once it has reached this
 * initial barrier.
 *
 * @param strategy the [SyncStrategy] to apply when emitting events.
 * @param alreadySent the [EventIdentifier] of already sent changes.
 */
private data class IncomingNew<T, C>(
    private val strategy: SyncStrategy,
    private val alreadySent: PersistentList<EventIdentifier>,
) : IncomingState<T, C>() {

  /**
   * Handles an advertisement message being successfully sent. This will update the list of
   * advertisements sent before the [Inc.Ready] message, and remove the site from the remaining
   * sites to send before the [Inc.Ready] message.
   *
   * @param msg the [Inc.Advertisement] message that was sent.
   */
  private fun handleAdvertisementSent(
      msg: Inc.Advertisement,
  ): Effect<IncomingState<T, C>> {
    val sent = alreadySent.add(EventIdentifier(msg.nextSeqno, msg.site))
    return Move(copy(alreadySent = sent))
  }

  /**
   * Handles the [Inc.Ready] message being sent, indicating that we are now done with the handshake
   * and can send events (and receive acknowledgements).
   */
  private fun handleReadySent(): Effect<IncomingState<T, C>> {
    return Move(IncomingSending(strategy = strategy, advertised = alreadySent))
  }

  /** Returns the next [Inc.Advertisement] message to send, if there's any. */
  private fun nextAdvertisementOrNull(log: ImmutableEventLog<*, *>): Inc.Advertisement? {
    val site = log.sites.minus(alreadySent.map { it.site }).lastOrNull() ?: return null
    return Inc.Advertisement(site, log.expected(site))
  }

  override suspend fun IncomingStepScope<T, C>.step(
      log: ImmutableEventLog<T, C>
  ): Effect<IncomingState<T, C>> {
    return select {

      // Priority is given to the reception of cancellation messages.
      onReceiveOrClosed { v ->
        when (v.valueOrNull) {
          null -> Terminate
          else -> Effect.MoveToError(IllegalStateException())
        }
      }

      // Try to send an advertisement, or Ready if we're done advertising sites.
      val pending = nextAdvertisementOrNull(log)
      if (pending != null) {
        // TODO : Investigate if this is an issue and if we need to better define sync semantics.
        // There is a small non-intuitive behavior here, if the log is updated via another site
        // before  the expected from another site is sent, and we may have some events with missing
        // dependencies (at the application level).
        //
        // On one hand, these missing dependencies should already be handled by the business logic,
        // but on the other hand, the semantics are not obvious to a user.
        onSend(pending) { handleAdvertisementSent(pending) }
      } else {
        onSend(Inc.Ready) { handleReadySent() }
      }
    }
  }
}

private data class IncomingSending<T, C>(
    private val strategy: SyncStrategy,
    private val advertised: PersistentList<EventIdentifier>,
    private val nextSequenceNumberPerSite: PersistentMap<SiteIdentifier, SequenceNumber> =
        persistentMapOf(),
    private val receivedCredits: PersistentMap<SiteIdentifier, UInt> = persistentMapOf(),
) : IncomingState<T, C>() {

  /** Returns the next [Inc.Advertisement] message to send, if there's any. */
  fun nextAdvertisementOrNull(log: ImmutableEventLog<T, C>): Inc.Advertisement? {
    // If we are syncing once, we should not issue new advertisements.
    if (strategy == SyncStrategy.Once) return null
    // Otherwise, we offer continuous sync and can send advertisements after the Inc.Ready.
    val advertisedSites = advertised.asSequence().map { it.site }
    val missing = log.sites.asSequence().minus(advertisedSites).firstOrNull() ?: return null
    val seqno = log.expected(missing)
    return Inc.Advertisement(site = missing, nextSeqno = seqno)
  }

  /**
   * Returns the [IncomingState] if the [Inc.Advertisement] for the given [msg] was successfully
   * processed.
   */
  fun handleAdvSent(msg: Inc.Advertisement): IncomingState<T, C> {
    return copy(advertised = advertised + EventIdentifier(msg.nextSeqno, msg.site))
  }

  /**
   * Returns the next [Inc.Event] message to send, if there's any.
   *
   * @param log the [ImmutableEventLog] that is in charge of generating the events.
   */
  @Suppress("DEPRECATION")
  fun nextEventOrNullContinuous(log: ImmutableEventLog<T, C>): Inc.Event<T>? {
    val event =
        receivedCredits
            .asSequence()
            .filter { (_, credits) -> credits > 0U }
            .filter { (site, _) -> site in advertised.asSequence().map { it.site } }
            .mapNotNull { (site, _) ->
              nextSequenceNumberPerSite[site]?.let { log.events(site, it) }
            }
            .flatten()
            .firstOrNull()
            ?: return null

    return Inc.Event(
        seqno = event.identifier.seqno,
        site = event.identifier.site,
        body = event.body,
    )
  }

  /**
   * Returns the [IncomingState] if the [Inc.Event] for the given [msg] was successfully processed.
   */
  fun handleEventSent(msg: Inc.Event<T>): IncomingState<T, C> {
    val remainingCredits = receivedCredits[msg.site]?.minus(1U) ?: 0U
    val nextSequenceNumber = maxOf(msg.seqno.inc(), Zero)
    return copy(
        nextSequenceNumberPerSite = nextSequenceNumberPerSite.put(msg.site, nextSequenceNumber),
        receivedCredits = receivedCredits.put(msg.site, remainingCredits),
    )
  }

  /** Returns the [IncomingState] if the [Out.Acknowledge] [msg] is received. */
  fun handleAcknowledgeReceived(msg: Out.Acknowledge): IncomingState<T, C> {
    return copy(
        nextSequenceNumberPerSite = nextSequenceNumberPerSite.put(msg.site, msg.nextSeqno),
        receivedCredits = receivedCredits.put(msg.site, 0U),
    )
  }

  /** Returns the [IncomingState] if the [Out.Request] [msg] is received. */
  fun handleRequestReceived(msg: Out.Request): IncomingState<T, C> {
    val currentCredits = receivedCredits[msg.site] ?: 0U
    val remainingCredits = currentCredits.plusBoundOverflows(msg.count)
    return copy(
        receivedCredits = receivedCredits.put(msg.site, remainingCredits),
    )
  }

  /**
   * Returns true iff all the events generated before the [Inc.Ready] advertisements have been
   * already properly sent.
   */
  fun hasSentAllPreReadyEvents(): Boolean {
    return advertised.all { adv ->
      val nextForSite = nextSequenceNumberPerSite[adv.site] ?: Zero
      nextForSite >= adv.seqno
    }
  }

  override suspend fun IncomingStepScope<T, C>.step(
      log: ImmutableEventLog<T, C>
  ): Effect<IncomingState<T, C>> {

    // Fast termination for SyncStrategy.Once.
    if (strategy == SyncStrategy.Once && hasSentAllPreReadyEvents()) return Terminate

    // TODO : Remove this.
    println("CHOICE")
    return select {
      onReceiveOrClosed { v ->
        when (val msg = v.valueOrNull) {
          is Out.Acknowledge -> Move(handleAcknowledgeReceived(msg))
          is Out.Request -> Move(handleRequestReceived(msg))
          null -> Terminate
        }
      }

      val event = nextEventOrNullContinuous(log)
      if (event != null) onSend(event) { Move(handleEventSent(event)) }

      val advertisement = nextAdvertisementOrNull(log)
      if (advertisement != null) onSend(advertisement) { Move(handleAdvSent(advertisement)) }

      // On new events, request another pass of the step.
      onInsert { Move(this@IncomingSending) }
      // TODO : Remove this.
      // onTimeout(10_000) {
      //   println(
      //       "DONE AFTER MEGA TIMEOUT $event $advertisement ${hasSentAllPreReadyEvents()} $strategy")
      //   Terminate
      // }
    }
  }
}
