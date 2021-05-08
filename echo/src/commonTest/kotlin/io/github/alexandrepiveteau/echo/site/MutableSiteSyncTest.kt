package io.github.alexandrepiveteau.echo.site

import io.github.alexandrepiveteau.echo.*
import io.github.alexandrepiveteau.echo.causal.SiteIdentifier.Companion.random
import io.github.alexandrepiveteau.echo.protocol.Message.Incoming
import io.github.alexandrepiveteau.echo.protocol.Message.Outgoing
import io.github.alexandrepiveteau.echo.sync.SyncStrategy.Continuous
import io.github.alexandrepiveteau.echo.sync.SyncStrategy.Once
import kotlin.test.Test

class MutableSiteSyncTest {

  @Test
  fun emptyLink_terminates() = suspendTest {
    val site = mutableSite<Nothing>(random())
    val link = link<Incoming<Nothing>, Outgoing<Nothing>> {}
    sync(site.incoming(), link)
  }

  @EchoSyncPreview
  @Test
  fun syncOnce_noProjection_terminates() = suspendTest {
    val alice = mutableSite<Unit>(random(), strategy = Once)
    val bob = mutableSite<Unit>(random(), strategy = Once)

    alice.event { yield(Unit) }
    sync(alice, bob)
    alice.event { yield(Unit) }
    bob.event { yield(Unit) }
    sync(alice, bob)
  }

  @EchoSyncPreview
  @Test
  // Bug reproducer
  fun syncOnce_projection_terminates() = suspendTest {
    val alice =
        mutableSite<Unit, Unit>(
            identifier = random(),
            strategy = Continuous,
            initial = Unit,
            projection = { _, _ -> },
        )
    val bob =
        mutableSite<Unit, Unit>(
            identifier = random(),
            strategy = Once,
            initial = Unit,
            projection = { _, _ -> },
        )

    alice.event { yield(Unit) }
    sync(alice, bob)
    println("Synced once")
    alice.event { yield(Unit) }
    //bob.event { yield(Unit) }
    sync(alice, bob)
    println("Synced twice")
  }
}
