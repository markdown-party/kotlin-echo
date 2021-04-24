package io.github.alexandrepiveteau.echo

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.fail
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.yield

class LinkBufferTest {

  @Test
  fun unbuffered_link_terminates() = suspendTest {
    val link = link<Int, Int> { it.collect() }
    assertEquals(emptyList(), link.talk(emptyFlow()).toList())
  }

  @Test
  fun buffered_channelLink_canSend() = suspendTest {
    val link = channelLink<Int, Int> { send(123) }
    assertEquals(listOf(123), link.talk(emptyFlow()).toList())
  }

  // TODO : Remove this OptIn annotation once Kotlin 1.5 is stable.
  @InternalCoroutinesApi
  @Test
  fun unbuffered_channelLink_cantSend() = suspendTest {
    val link =
        channelLink<Int, Int> {
              yield() // The produceIn(..) call of channelLink should run first.
              select {
                it.onReceiveOrClosed { v -> assertNull(v.valueOrNull) }
                onSend(123) { fail() }
              }
            }
            .buffer(Channel.RENDEZVOUS)
    assertEquals(emptyList(), link.talk(emptyFlow()).toList())
  }
}
