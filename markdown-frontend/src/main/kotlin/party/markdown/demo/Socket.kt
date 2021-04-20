package party.markdown.demo

import io.github.alexandrepiveteau.echo.causal.SiteIdentifier.Companion.random
import io.github.alexandrepiveteau.echo.ktor.exchange
import io.github.alexandrepiveteau.echo.mutableSite
import io.github.alexandrepiveteau.echo.protocol.decode
import io.github.alexandrepiveteau.echo.sync
import io.ktor.client.*
import io.ktor.client.engine.js.*
import io.ktor.client.features.websocket.*
import io.ktor.client.request.*
import kotlinx.coroutines.launch
import kotlinx.html.js.onClickFunction
import party.markdown.MarkdownEvent
import party.markdown.MarkdownEvent.Decrement
import party.markdown.MarkdownEvent.Increment
import party.markdown.MarkdownProjection
import party.markdown.react.useCoroutineScope
import party.markdown.react.useFlow
import party.markdown.react.useLaunchedEffect
import react.*
import react.dom.button
import react.dom.h1

private val Client = HttpClient(Js) { install(WebSockets) }
private val Remote =
    Client.exchange(
            receiver = {
              port = 443
              url {
                host = "api.markdown.party"
                path("receiver")
              }
            },
            sender = {
              port = 443
              url {
                host = "api.markdown.party"
                path("sender")
              }
            },
        )
        .decode(MarkdownEvent)

private val State =
    mutableSite(
        identifier = random(),
        initial = 0,
        projection = MarkdownProjection,
    )

/** A [functionalComponent] that displays a very simple websockets demonstration. */
private val socket =
    functionalComponent<RProps> {
      val scope = useCoroutineScope()
      useLaunchedEffect(listOf()) { sync(Remote, State) }

      val total = useFlow(0, State.value)

      h1 { +"Current total is $total" }
      button {
        attrs { onClickFunction = { scope.launch { State.event { yield(Decrement) } } } }
        +"Decrement"
      }
      button {
        attrs { onClickFunction = { scope.launch { State.event { yield(Increment) } } } }
        +"Increment"
      }
    }

/** Adds a new sockets component. */
fun RBuilder.sockets(): ReactElement = child(socket)