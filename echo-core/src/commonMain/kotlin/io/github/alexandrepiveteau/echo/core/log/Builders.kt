package io.github.alexandrepiveteau.echo.core.log

import io.github.alexandrepiveteau.echo.core.causality.EventIdentifier

/**
 * Creates a new [MutableHistory], with an aggregate with an [initial] value and a [projection] for
 * incremental changes.
 *
 * @param initial the initial aggregate value.
 * @param projection the [MutableProjection] value.
 *
 * @param T the type of the aggregate.
 */
fun <T> mutableHistoryOf(
    initial: T,
    projection: MutableProjection<T>,
): MutableHistory<T> = MutableHistoryImpl(initial, projection)

/**
 * Creates a new [MutableHistory], with an aggregate with an [initial] value and a [projection] for
 * incremental changes.
 *
 * @param initial the initial aggregate value.
 * @param projection the [MutableProjection] value.
 * @param events some events to pre-populate the history.
 *
 * @param T the type of the aggregate.
 */
fun <T> mutableHistoryOf(
    initial: T,
    projection: MutableProjection<T>,
    vararg events: Pair<EventIdentifier, ByteArray>,
): MutableHistory<T> =
    MutableHistoryImpl(initial, projection).apply {
      for ((id, body) in events) {
        insert(id.seqno, id.site, body)
      }
    }

/** Creates a new [MutableEventLog], with no aggregate. */
fun mutableEventLogOf(): MutableEventLog = mutableHistoryOf(NoModel, NoProjection)

/**
 * Creates a new [MutableEventLog], with no aggregate.
 *
 * @param events some events to pre-populate the history.
 */
fun mutableEventLogOf(
    vararg events: Pair<EventIdentifier, ByteArray>,
): MutableEventLog = mutableHistoryOf(NoModel, NoProjection, *events)

// An object which represents the absence of an aggregated model.
private object NoModel

// An object which represents the absence of an aggregating projection.
private object NoProjection : MutableProjection<NoModel> {

  override fun ChangeScope.forward(
      model: NoModel,
      identifier: EventIdentifier,
      data: ByteArray,
      from: Int,
      until: Int,
  ): NoModel = NoModel

  override fun backward(
      model: NoModel,
      identifier: EventIdentifier,
      data: ByteArray,
      from: Int,
      until: Int,
      changeData: ByteArray,
      changeFrom: Int,
      changeUntil: Int,
  ): NoModel = NoModel
}
