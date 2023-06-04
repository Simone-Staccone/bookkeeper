package org.apache.bookkeeper.proto;

import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import junit.framework.TestCase;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.ReadOnlyBookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

public class BookieRequestProcessorTest extends TestCase {
    private final ChannelGroup channelGroup = new DefaultChannelGroup(null);

//    @Test
//    public void testConstructLongPollThreads() throws Exception {
//        // long poll threads == read threads
//
//        ServerConfiguration conf = new ServerConfiguration();
//        try (BookieRequestProcessor processor = new BookieRequestProcessor(
//                conf, mock(Bookie.class), NullStatsLogger.INSTANCE, null, UnpooledByteBufAllocator.DEFAULT,
//                channelGroup)) {
//            assertSame(processor.getReadThreadPool(), processor.getLongPollThreadPool());
//        }
//
//        // force create long poll threads if there is no read threads
//        conf = new ServerConfiguration();
//        conf.setNumReadWorkerThreads(0);
//        try (BookieRequestProcessor processor = new BookieRequestProcessor(
//                conf, mock(Bookie.class), NullStatsLogger.INSTANCE, null, UnpooledByteBufAllocator.DEFAULT,
//                channelGroup)) {
//            assertNull(processor.getReadThreadPool());
//            assertNotNull(processor.getLongPollThreadPool());
//        }
//
//        // long poll threads and no read threads
//        conf = new ServerConfiguration();
//        conf.setNumReadWorkerThreads(2);
//        conf.setNumLongPollWorkerThreads(2);
//        try (BookieRequestProcessor processor = new BookieRequestProcessor(
//                conf, mock(Bookie.class), NullStatsLogger.INSTANCE, null, UnpooledByteBufAllocator.DEFAULT,
//                channelGroup)) {
//            assertNotNull(processor.getReadThreadPool());
//            assertNotNull(processor.getLongPollThreadPool());
//            assertNotSame(processor.getReadThreadPool(), processor.getLongPollThreadPool());
//        }
//    }
}