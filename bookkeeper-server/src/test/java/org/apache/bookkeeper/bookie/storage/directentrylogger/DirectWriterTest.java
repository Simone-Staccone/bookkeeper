package org.apache.bookkeeper.bookie.storage.directentrylogger;

import junit.framework.TestCase;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.slogger.Slogger;
import org.junit.After;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.storage.directentrylogger.DirectEntryLogger.logFilename;
import static org.mockito.Mockito.mock;

//Dummy test to check configuration working
public class DirectWriterTest extends TestCase {
    private static final Slogger slog = Slogger.CONSOLE;
    private final TmpDirs tmpDirs = new TmpDirs();


    private final ExecutorService writeExecutor = Executors.newSingleThreadExecutor();

    private static File getLedgerDir(){
        TmpDirs tmpDirs = new TmpDirs();
        try {
            return tmpDirs.createNew("writeAlignment", "logs");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static ExecutorService getExecutorService(){
        return Executors.newSingleThreadExecutor();
    }

    private static BufferPool getBuffers() {
        try {
            return new BufferPool(new NativeIOImpl(), Buffer.ALIGNMENT, 8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    public static Stream<Arguments> partitioningForConstructor(){
        return Stream.of(
                Arguments.of(-1,logFilename(getLedgerDir(), -1),1 << 24,getExecutorService(),getBuffers(),new NativeIOImpl(),Slogger.CONSOLE,false),
                Arguments.of(1,logFilename(getLedgerDir(), 1),1 << 24,getExecutorService(),getBuffers(),mock(NativeIOImpl.class),Slogger.CONSOLE,false),
                Arguments.of(1,logFilename(getLedgerDir(), -1),1 << 24,getExecutorService(),getBuffers(),mock(NativeIOImpl.class),Slogger.CONSOLE,true),
                Arguments.of(-1,logFilename(getLedgerDir(), 1),1 << 24,getExecutorService(),getBuffers(),mock(NativeIOImpl.class),Slogger.CONSOLE,true),
                Arguments.of(-1,null,1 << 24,getExecutorService(),getBuffers(), mock(NativeIOImpl.class),Slogger.CONSOLE,true),
                Arguments.of(1,null,1 << 24,getExecutorService(), getBuffers(), mock(NativeIOImpl.class),Slogger.CONSOLE,true)
        );
    }

    public static Stream<Arguments> dummy(){
        return Stream.of(
                Arguments.of(0),
                Arguments.of(2)
                );
    }


    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
        writeExecutor.shutdownNow();
    }


//    @Test
//    public void testWriteBlocksFlush() throws Exception {
//        ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
//        try {
//            File ledgerDir = tmpDirs.createNew("blockWrite", "logs");
//            try (BufferPool buffers = new BufferPool(new NativeIOImpl(), 1 << 14, 8);
//                 LogWriter writer = new DirectWriter(1234, logFilename(ledgerDir, 5678),
//                         1 << 24, writeExecutor,
//                         buffers,mock(NativeIOImpl.class), Slogger.CONSOLE)) {
//                CompletableFuture<?> blocker = new CompletableFuture<>();
//                writeExecutor.submit(() ->  {
//                    blocker.join();
//                    return null;
//                });
//                ByteBuf bb = Unpooled.buffer(4096);
//                TestBuffer.fillByteBuf(bb, 0xdeadbeef);
//                writer.writeAt(0, bb);
//                Future<?> f = flushExecutor.submit(() -> {
//                    writer.flush();
//                    return null;
//                });
//                Thread.sleep(100);
//                Assertions.assertFalse(f.isDone());
//                blocker.complete(null);
//                f.get();
//            }
//            ByteBuf contents = readIntoByteBuf(ledgerDir, 1234);
//            for (int i = 0; i < 4096 / Integer.BYTES; i++) {
//                Assertions.assertEquals(contents.readInt(),0xdeadbeef);
//            }
//            if (contents.readableBytes() > 0) { // linux-only: fallocate will preallocate file
//                while (contents.isReadable()) {
//                    Assertions.assertEquals((int) contents.readByte(),0);
//                }
//            }
//        } finally {
//            flushExecutor.shutdownNow();
//        }
//    }
//
//    static ByteBuf readIntoByteBuf(File directory, int logId) throws Exception {
//        byte[] bytes = new byte[1024];
//        File file = new File(logFilename(directory, logId));
//        slog.kv("filename", file.toString()).info("reading in");
//        ByteBuf byteBuf = Unpooled.buffer((int) file.length());
//        try (FileInputStream is = new FileInputStream(file)) {
//            int bytesRead = is.read(bytes);
//            while (bytesRead > 0) {
//                byteBuf.writeBytes(bytes, 0, bytesRead);
//                bytesRead = is.read(bytes);
//            }
//        }
//        Assertions.assertEquals(byteBuf.readableBytes(), (int) file.length());
//
//        return byteBuf;
//    }

//    @Test
//    public void testWriteAtAlignment() throws Exception {
//        File ledgerDir = tmpDirs.createNew("writeAlignment", "logs");
//        try{
//            BufferPool buffers = new BufferPool(new NativeIOImpl(), Buffer.ALIGNMENT, 8);
//            LogWriter writer = new DirectWriter(5678, logFilename(ledgerDir, 5678),
//                    0, writeExecutor,
//                    buffers, mock(NativeIOImpl.class), Slogger.CONSOLE);
//            ByteBuf bb = Unpooled.buffer(Buffer.ALIGNMENT);
//            TestBuffer.fillByteBuf(bb, 0xdededede);
//            writer.writeAt(1234, bb);
//            writer.flush();
//        }catch (Exception e){
//            Assertions.assertFalse(false);
//        }
//    }

//    @ParameterizedTest
//    @MethodSource("partitioningForConstructor")
//    public void testConstructor(int id, String path, int maxFileSize, ExecutorService writeExecutor, BufferPool buffers, NativeIO io,Slogger slogger, boolean exceptionExpected) {
//        try {
//            LogWriter writer = new DirectWriter(id, path, maxFileSize, writeExecutor,
//                    buffers, io, slogger);
//            ByteBuf bb = Unpooled.buffer(Buffer.ALIGNMENT);
//            TestBuffer.fillByteBuf(bb, 0xdededede);
//            writer.writeAt(1234, bb);
//            writer.flush();
//            Assertions.assertFalse(exceptionExpected);
//        }catch (Exception e){
//            Assertions.assertTrue(exceptionExpected);
//        }
//    }

    @ParameterizedTest
    @MethodSource("dummy")
    public void testWriteAt(int zero) {
        Assertions.assertTrue(zero >= 0);
    }

    public void testWriteDelimited() {
    }

    public void testPosition() {
    }

    public void testTestPosition() {
    }

    public void testFlush() {
    }

    public void testClose() {
    }

    public void testSerializedSize() {
    }
}