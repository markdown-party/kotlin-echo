package io.github.alexandrepiveteau.echo.core.buffer

import io.github.alexandrepiveteau.echo.core.causality.EventIdentifier
import io.github.alexandrepiveteau.echo.core.causality.EventIdentifierArray

/**
 * An interface representing a mutable gap buffer, containing a sequence of [EventIdentifier], which
 * is optimized for chunk insertions and removals.
 *
 * Gap buffers are data structures which support amortized constant-time consecutive insertions,
 * similarly to an array-based list, but at arbitrary buffer positions. They may be used to store a
 * sequence of items which are known to be inserted group-wise.
 *
 * In a gap buffer, positions are known as offsets. An offset is semantically identical to an index
 * in an array, except that it jumps over the gap.
 */
interface MutableEventIdentifierGapBuffer {

  /** The backing [EventIdentifierArray]. */
  val backing: EventIdentifierArray

  /** How many items there are in the gap buffer. */
  val size: Int

  /** Some meta-data about the [Gap]. This may be useful for specific optimizations. */
  val gap: Gap

  /**
   * Gets the [EventIdentifier] at the given [offset].
   *
   * @throws IllegalArgumentException if the [offset] is out of bounds.
   */
  operator fun get(
      offset: Int,
  ): EventIdentifier

  /**
   * Sets the [EventIdentifier] at the given [offset].
   *
   * @throws IllegalArgumentException if the [offset] is out of bounds.
   */
  operator fun set(
      offset: Int,
      value: EventIdentifier,
  )

  /**
   * Pushes the given [EventIdentifier] at the provided offset (defaults to the end of the buffer).
   *
   * This operation may move the gap.
   */
  fun push(
      value: EventIdentifier,
      offset: Int = size,
  )

  /**
   * Pushes the given [EventIdentifierArray] at the provided offset (defaults to the end of the
   * buffer).
   *
   * This operation may move the gap.
   */
  fun push(
      array: EventIdentifierArray,
      offset: Int = size,
      startIndex: Int = 0,
      endIndex: Int = array.size,
  )

  /**
   * Copies from the gap buffer into the provided [EventIdentifierArray].
   *
   * @param array the [EventIdentifierArray] into which data should be copied.
   * @param destinationOffset the destination index at which the copy starts.
   * @param startOffset the offset at which copy starts, in the gap buffer.
   *
   * This operation may move the gap.
   */
  fun copyInto(
      array: EventIdentifierArray,
      destinationOffset: Int = 0,
      startOffset: Int = 0,
      endOffset: Int = size,
  ): EventIdentifierArray

  /**
   * Removes the given count of items from the gap buffer at the given offset.
   *
   * This operation may move the gap.
   */
  fun remove(
      offset: Int,
      size: Int = 1,
  ): EventIdentifierArray

  /**
   * Removes the whole gap buffer, clearing the current data. This operation takes a constant time
   * and does not require moving the gap.
   */
  fun clear()
}
