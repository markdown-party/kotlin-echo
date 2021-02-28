@file:OptIn(ExperimentalCoroutinesApi::class)

package markdown.echo.memory

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel.Factory.RENDEZVOUS
import kotlinx.coroutines.channels.receiveOrNull
import kotlinx.coroutines.runBlocking
import markdown.echo.*
import markdown.echo.causal.EventIdentifier
import markdown.echo.causal.SequenceNumber
import markdown.echo.causal.SiteIdentifier
import markdown.echo.memory.log.mutableEventLogOf
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import markdown.echo.Message.V1.Incoming as I
import markdown.echo.Message.V1.Outgoing as O

@OptIn(EchoPreview::class)
class MemoryEchoTest {

    @Test
    fun `Only Done works on buffered incoming`() = runBlocking {
        val echo = Echo.memory<Nothing>(SiteIdentifier(123))
        val exchange = channelExchange<I<Nothing>, O<Nothing>> { incoming ->
            assertTrue(incoming.receive() is I.Ready)
            send(O.Done)
            assertTrue(incoming.receive() is I.Done)
            assertNull(incoming.receiveOrNull())
        }
        sync(echo.incoming(), exchange)
    }

    @Test
    fun `Only Done works on rendezvous incoming`() = runBlocking {
        val echo = Echo.memory<Nothing>(SiteIdentifier(123)).buffer(RENDEZVOUS)
        val exchange = channelExchange<I<Nothing>, O<Nothing>> { incoming ->
            assertTrue(incoming.receive() is I.Ready)
            send(O.Done)
            assertTrue(incoming.receive() is I.Done)
            assertNull(incoming.receiveOrNull())
        }.buffer(RENDEZVOUS)
        sync(echo.incoming(), exchange)
    }

    @Test
    fun `Advertises one event and cancels if rendezvous and not empty`() = runBlocking {
        val seqno = SequenceNumber(123)
        val site = SiteIdentifier(456)
        val log = mutableEventLogOf(EventIdentifier(seqno, site) to 42)
        val echo = Echo.memory(site, log).buffer(RENDEZVOUS)
        val exchange = channelExchange<I<Int>, O<Int>> { incoming ->
            assertEquals(I.Advertisement(site), incoming.receive())
            assertEquals(I.Ready, incoming.receive())
            send(O.Done)
            assertTrue(incoming.receive() is I.Done)
            assertNull(incoming.receiveOrNull())
        }.buffer(RENDEZVOUS)
        sync(echo.incoming(), exchange)
    }
}
