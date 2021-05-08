@file:OptIn(EchoSyncPreview::class)

package io.github.alexandrepiveteau.echo.demo.counter

import io.github.alexandrepiveteau.echo.EchoSyncPreview
import io.github.alexandrepiveteau.echo.causal.SiteIdentifier
import io.github.alexandrepiveteau.echo.demo.counter.PNCounterEvent.Decrement
import io.github.alexandrepiveteau.echo.demo.counter.PNCounterEvent.Increment
import io.github.alexandrepiveteau.echo.logs.EventLog.IndexedEvent
import io.github.alexandrepiveteau.echo.mutableSite
import io.github.alexandrepiveteau.echo.projections.OneWayProjection
import io.github.alexandrepiveteau.echo.suspendTest
import io.github.alexandrepiveteau.echo.sync
import io.github.alexandrepiveteau.echo.sync.SyncStrategy.Once
import kotlin.test.Test
import kotlinx.coroutines.flow.first

private sealed class PNCounterEvent {
  object Increment : PNCounterEvent() {
    override fun toString(): String = "PNCounterEvent.Increment"
  }
  object Decrement : PNCounterEvent() {
    override fun toString(): String = "PNCounterEvent.Decrement"
  }
}

private val PNProjection =
    OneWayProjection<Int, IndexedEvent<PNCounterEvent>> { event, sum ->
      when (event.body) {
        is Increment -> sum + 1
        is Decrement -> sum - 1
      }
    }

class PNCounterTest {

  @Test
  fun twoSitesCanCreateASharedCounter_andSync(): Unit = suspendTest {
    val alice =
        mutableSite(
            identifier = SiteIdentifier(0),
            initial = 0,
            strategy = Once,
            projection = PNProjection,
        )
    val bob =
        mutableSite(
            identifier = SiteIdentifier(1),
            initial = 0,
            strategy = Once,
            projection = PNProjection,
        )

    alice.event {
      yield(Decrement)
      yield(Increment)
      yield(Decrement)
    }

    bob.event {
      yield(Increment)
      yield(Increment)
    }

    // Sync both sites (with a timeout, since by default they'll keep the connection open until
    // either side cancels).
    sync(alice, bob)

    // Finally, look at the resulting set of both sites, and make sure they eventually reach the
    // right result.
    alice.value.first { it == 1 }
    bob.value.first { it == 1 }
  }
}
