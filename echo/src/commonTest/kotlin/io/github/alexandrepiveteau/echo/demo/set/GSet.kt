@file:OptIn(EchoSyncPreview::class)

package io.github.alexandrepiveteau.echo.demo.set

import io.github.alexandrepiveteau.echo.EchoSyncPreview
import io.github.alexandrepiveteau.echo.causal.SiteIdentifier
import io.github.alexandrepiveteau.echo.logs.EventLog.IndexedEvent
import io.github.alexandrepiveteau.echo.mutableSite
import io.github.alexandrepiveteau.echo.projections.OneWayProjection
import io.github.alexandrepiveteau.echo.suspendTest
import io.github.alexandrepiveteau.echo.sync
import io.github.alexandrepiveteau.echo.sync.SyncStrategy
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.flow.first

private sealed class GSetEvent<out T> {
  data class Add<out T>(val item: T) : GSetEvent<T>()
}

private fun <T> gSetProjection() =
    OneWayProjection<Set<T>, IndexedEvent<GSetEvent<T>>> { event, model ->
      when (val body = event.body) {
        // Add events, and duplicate insertions.
        is GSetEvent.Add -> model + body.item
      }
    }

class GSetTest {

  @Test
  fun oneSite_canYieldEvents() = suspendTest {
    val alice = SiteIdentifier.random()
    val echo =
        mutableSite(
            identifier = alice,
            initial = emptySet<Int>(),
            projection = gSetProjection(),
        )

    echo.event {
      yield(GSetEvent.Add(1))
      yield(GSetEvent.Add(2))
      yield(GSetEvent.Add(3))
    }

    val result = echo.value.first { it.size == 3 }
    assertEquals(setOf(1, 2, 3), result)
  }

  @Test
  fun twoSites_converge() = suspendTest {
    // Create Alice, our first site.
    val alice = SiteIdentifier.random()
    val aliceEcho =
        mutableSite(
            identifier = alice,
            initial = emptySet<Int>(),
            strategy = SyncStrategy.Once,
            projection = gSetProjection(),
        )

    // Create Bob, our second site.
    val bob = SiteIdentifier.random()
    val bobEcho =
        mutableSite(
            identifier = bob,
            initial = emptySet<Int>(),
            strategy = SyncStrategy.Once,
            projection = gSetProjection(),
        )

    // Alice adds the elements 1 and 2.
    aliceEcho.event {
      yield(GSetEvent.Add(1))
      yield(GSetEvent.Add(2))
    }

    // Bob adds the elements 2, 3 and 4 concurrently.
    bobEcho.event {
      yield(GSetEvent.Add(2))
      yield(GSetEvent.Add(3))
      yield(GSetEvent.Add(4))
    }

    val aliceBeforeSync = aliceEcho.value.first { it.size == 2 }
    val bobBeforeSync = bobEcho.value.first { it.size == 3 }

    // Before sync, both sites have not merged their operations yet.
    assertEquals(setOf(1, 2), aliceBeforeSync)
    assertEquals(setOf(2, 3, 4), bobBeforeSync)

    // Sync both sites.
    sync(aliceEcho, bobEcho)

    // Finally, look at the resulting set of both sites.
    val aliceAfterSync = aliceEcho.value.first { it.size == 4 }
    val bobAfterSync = aliceEcho.value.first { it.size == 4 }

    // All the events are properly synced.
    val expected = setOf(1, 2, 3, 4)
    assertEquals(expected, aliceAfterSync)
    assertEquals(expected, bobAfterSync)
  }
}
