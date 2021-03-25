package markdown.echo.projections

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.selects.select
import markdown.echo.EchoEventLogPreview
import markdown.echo.Message.V1.Incoming as I
import markdown.echo.Message.V1.Outgoing as O
import markdown.echo.ReceiveExchange
import markdown.echo.causal.EventIdentifier
import markdown.echo.causal.SequenceNumber.Companion.Zero
import markdown.echo.causal.SiteIdentifier
import markdown.echo.logs.EventLog
import markdown.echo.logs.MutableEventLog
import markdown.echo.logs.mutableEventLogOf

/**
 * Projects the provided [ReceiveExchange] instance with a [OneWayProjection].
 *
 * Usually, you would want to use a more efficient implementation of projection.
 *
 * @param M the type of the model generated by the [OneWayProjection].
 * @param T the type of the events managed by the [ReceiveExchange].
 */
fun <M, T> ReceiveExchange<I<T>, O<T>>.projection(
    initial: M,
    transform: OneWayProjection<M, T>,
): Flow<M> = projectWithIdentifiers(initial) { (_, e), m -> transform.forward(e, m) }

/**
 * Projects the provided [ReceiveExchange] instance with a [OneWayProjection].
 *
 * Usually, you would want to use a more efficient implementation of projection.
 *
 * @param M the type of the model generated by the [OneWayProjection].
 * @param T the type of the events managed by the [ReceiveExchange].
 */
@OptIn(
    ExperimentalCoroutinesApi::class,
    InternalCoroutinesApi::class,
)
fun <M, T> ReceiveExchange<I<T>, O<T>>.projectWithIdentifiers(
    initial: M,
    transform: OneWayProjection<M, Pair<EventIdentifier, T>>,
): Flow<M> = channelFlow {

  // Emit the initial value of the Model.
  send(initial)

  // Use channels for communication.
  val incoming = Channel<I<T>>()
  val outgoing =
      produce<O<T>> {

        // Iterate on the State until we are completed.
        var state: State<T> = State.Preparing(advertisedSites = mutableListOf())
        while (state != State.Completed && !(isClosedForSend && incoming.isClosedForReceive)) {
          state =
              when (val s = state) {
                is State.Preparing ->
                    select {
                      incoming.onReceiveOrClosed { v ->
                        when (val msg = v.valueOrNull) {
                          is I.Done, null -> State.Cancelling
                          is I.Advertisement -> {
                            s.advertisedSites += msg.site
                            return@onReceiveOrClosed s // Updated a mutable state.
                          }
                          is I.Event -> error("Event should not be send before Ready.")
                          is I.Ready ->
                              State.Listening(
                                  advertisedSites = s.advertisedSites,
                                  log = mutableEventLogOf(),
                              )
                        }
                      }
                    }
                is State.Listening ->
                    select {
                      val request = s.advertisedSites.lastOrNull()
                      if (request != null) {
                        @OptIn(EchoEventLogPreview::class)
                        onSend(
                            O.Request(
                                nextForAll = s.log.expected,
                                nextForSite = Zero,
                                site = request,
                            )) {
                          s.advertisedSites.removeLast()
                          return@onSend s // Updated a mutable state.
                        }
                      }

                      incoming.onReceiveOrClosed { v ->
                        when (val msg = v.valueOrNull) {
                          is I.Done, null -> State.Cancelling
                          is I.Advertisement -> {
                            s.advertisedSites += msg.site
                            return@onReceiveOrClosed s // Updated a mutable state.
                          }
                          is I.Event -> {
                            // Issue the new state now.
                            s.log[msg.seqno, msg.site] = msg.body
                            this@channelFlow.send(s.log.aggregate(initial, transform))
                            return@onReceiveOrClosed s // Updated a mutable state.
                          }
                          is I.Ready -> error("Ready should not be issued twice.")
                        }
                      }
                    }

                // We can send a Done message, and move to Completed.
                is State.Cancelling -> select { onSend(O.Done) { State.Completed } }

                // This should never be called. It's included for completeness.
                is State.Completed -> State.Completed
              }
        }
      }

  // Start the exchange between both sites.
  incoming()
      .talk(outgoing.consumeAsFlow())
      .onEach { incoming.send(it) }
      .onCompletion { incoming.close() }
      .collect()
}

private sealed class State<out T> {

  data class Preparing(
      val advertisedSites: MutableList<SiteIdentifier>,
  ) : State<Nothing>()

  data class Listening<T>(
      val advertisedSites: MutableList<SiteIdentifier>,
      val log: MutableEventLog<T>,
  ) : State<T>()

  object Cancelling : State<Nothing>()
  object Completed : State<Nothing>()
}

@OptIn(EchoEventLogPreview::class)
private fun <M, T> EventLog<T>.aggregate(
    model: M,
    transform: OneWayProjection<M, Pair<EventIdentifier, T>>,
): M {
  return foldl(model, transform::forward)
}
