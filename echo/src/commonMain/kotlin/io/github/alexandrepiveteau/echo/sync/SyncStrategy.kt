package io.github.alexandrepiveteau.echo.sync

import io.github.alexandrepiveteau.echo.EchoSyncPreview
import io.github.alexandrepiveteau.echo.sync.SyncStrategy.Continuous
import io.github.alexandrepiveteau.echo.sync.SyncStrategy.Once

/**
 * A strategy used for replicating content across sites. Two modes exist : [Continuous], where sites
 * will keep syncing until the links are manually, and [Once], where sites will cancel the link once
 * the baseline defined by advertisement messages is reached.
 */
// TODO : Offer a flexible behavior, where sites may dynamically choose the strategy.
@EchoSyncPreview
enum class SyncStrategy {

  /** Sync forever. */
  Continuous,

  /** Sync the elements already on the sites when sync was initiated. */
  Once,
}
