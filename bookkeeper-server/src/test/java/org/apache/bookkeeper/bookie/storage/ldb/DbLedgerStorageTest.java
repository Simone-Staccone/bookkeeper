/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import com.beust.jcommander.internal.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.After;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Unit test for {@link DbLedgerStorage}.
 */
public class DbLedgerStorageTest {
    private static final Logger log = LoggerFactory.getLogger(DbLedgerStorageTest.class);
    protected DbLedgerStorage storage;
    protected File tmpDir;
    protected LedgerDirsManager ledgerDirsManager;
    protected ServerConfiguration conf;
    private static final int BUFF_SIZE = 128;

    @BeforeEach
    void setup() throws Exception {
        tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000; //Time of garbage collector to delete entries that aren't associated anymore to an active ledger
        conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName()); //Set this class as the persistence one
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });


        BookieImpl bookie = new TestBookieImpl(conf);

        ledgerDirsManager = bookie.getLedgerDirsManager();
        storage = (DbLedgerStorage) bookie.getLedgerStorage();

        storage.getLedgerStorageList().forEach(singleDirectoryDbLedgerStorage -> {
            assertTrue(singleDirectoryDbLedgerStorage.getEntryLogger() instanceof DefaultEntryLogger);
        });
    }



    @AfterEach
    public void teardown() throws Exception {
        storage.shutdown();
        tmpDir.delete();
    }


    public static Stream<Arguments> addEntryPartition() {
        return Stream.of(
                Arguments.of(null, true),
                Arguments.of(Unpooled.buffer(BUFF_SIZE),false),
                Arguments.of(Unpooled.buffer(0),false)
        );
    }

    @ParameterizedTest
    @MethodSource("addEntryPartition")
    public void addEntryTest(ByteBuf entry,boolean expectedException){
        try {
            entry.writeLong(4); // ledger id
            entry.writeLong(1); // entry id
            entry.writeBytes("entry-example-1".getBytes());
            storage.addEntry(entry);
            storage.flush();


            assertEquals(entry,storage.getEntry(4,1)); //Verify that just added entry is sotred in test DB
            Assertions.assertFalse(expectedException);
        } catch (Exception e) {
            Assertions.assertTrue(expectedException);
        }
    }

    @ParameterizedTest
    @MethodSource("addEntryPartition") //entry id must be unique
    public void duplicateAdd(ByteBuf entry,boolean expectedException){
        try {
            entry.writeLong(4); // ledger id
            entry.writeLong(1); // entry id
            entry.writeBytes("entry-example-1".getBytes());
            storage.addEntry(entry);
            storage.flush();
            storage.addEntry(entry);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (BookieException | NullPointerException e) {
            Assertions.assertTrue(expectedException);
        }
    }


    @Test
    public void testLimboState() {
        try {
            storage.setMasterKey(1,"key".getBytes());

            ByteBuf entry = Unpooled.buffer(128);
            entry.writeLong(1);
            entry.writeLong(1);
            entry.writeBytes("entry".getBytes());

            storage.addEntry(entry);
            storage.flush();
            storage.setLimboState(1);

            Assertions.assertThrows(BookieException.class, () -> storage.isFenced(1));


            storage.setMasterKey(2,"key".getBytes());

            ByteBuf entry2 = Unpooled.buffer(128);
            entry2.writeLong(2);
            entry2.writeLong(1);
            entry2.writeBytes("entry2".getBytes());

            storage.addEntry(entry2);
            storage.flush();
            storage.setFenced(2);

            storage.setLimboState(2);


            Assertions.assertDoesNotThrow(() -> storage.isFenced(2));
        } catch (IOException | BookieException e) {
            e.printStackTrace();
        }


    }


    public static Stream<Arguments> getEntryPartition() {
        return Stream.of(
                Arguments.of(1,1, false),
                Arguments.of(1,0, false),
                Arguments.of(1,-1, true),
                Arguments.of(0,1, false),
                Arguments.of(0,0, false),
                Arguments.of(0,-1, true),
                Arguments.of(-1,1, true),
                Arguments.of(-1,0, true),
                Arguments.of(-1,-1, true)
        );
    }

    @ParameterizedTest
    @MethodSource("getEntryPartition")
    public void getEntryTest(int ledgerId, int entryId ,boolean expectedException){
        try {
            ByteBuf entry = Unpooled.buffer(BUFF_SIZE);
            entry.writeLong(ledgerId); // ledger id
            entry.writeLong(entryId); // entry id
            entry.writeBytes("entry-example-2".getBytes());
            storage.addEntry(entry);
            storage.flush();
            assertEquals(entry,storage.getEntry(ledgerId,entryId)); //Verify that just added entry is sotred in test DB
            Assertions.assertFalse(expectedException);
        } catch (Exception e) {
            Assertions.assertTrue(expectedException);
        }

    }


    private static ServerConfiguration getServerConfiguration(int testCase) throws IOException {
        int gcWaitTime;
        String ledgerStorageClass;
        ServerConfiguration serverConfiguration = TestBKConfiguration.newServerConfiguration();

        switch (testCase){
            case(1):
                gcWaitTime = 1000;
                ledgerStorageClass = DbLedgerStorage.class.getName();
                break;
            case(2):
                gcWaitTime = -1;
                ledgerStorageClass = DbLedgerStorage.class.getName();
                break;
            case(3):
                gcWaitTime = 0;
                ledgerStorageClass = DbLedgerStorage.class.getName();
                break;
            case(4):
                gcWaitTime = 1000;
                ledgerStorageClass = InterleavedLedgerStorage.class.getName();
                break;
            case(5):
                gcWaitTime = -1;
                ledgerStorageClass = InterleavedLedgerStorage.class.getName();
                break;
            case(6):
                gcWaitTime = 0;
                ledgerStorageClass = InterleavedLedgerStorage.class.getName();
                break;
            case(7):
                gcWaitTime = 1000;
                ledgerStorageClass = SortedLedgerStorage.class.getName();
                break;
            case(8):
                gcWaitTime = -1;
                ledgerStorageClass = SortedLedgerStorage.class.getName();
                break;
            case(9):
                gcWaitTime = 0;
                ledgerStorageClass = SortedLedgerStorage.class.getName();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + testCase);
        }

        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);


        serverConfiguration.setGcWaitTime(gcWaitTime);
        serverConfiguration.setLedgerStorageClass(ledgerStorageClass); //Set this class as the persistence one
        serverConfiguration.setLedgerDirNames(new String[] { tmpDir.toString() });
        return serverConfiguration;
    }


    private static SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor getLedgerLoggerProcessor() {
        return (currentEntry, entryLodId, position) -> System.out
                .println("entry " + currentEntry + "\t:\t(log: " + entryLodId + ", pos: " + position + ")");
    }



    public static Stream<Arguments> readLedgerIndexEntriesPartition() throws IOException {
        return Stream.of(
                Arguments.of(1,getServerConfiguration(1),getLedgerLoggerProcessor(), false),
                Arguments.of(1,getServerConfiguration(2),getLedgerLoggerProcessor(), true),
                Arguments.of(1,getServerConfiguration(3),getLedgerLoggerProcessor(), true),
                Arguments.of(1,getServerConfiguration(4),getLedgerLoggerProcessor(), true),
                Arguments.of(1,getServerConfiguration(5),getLedgerLoggerProcessor(), true),
                Arguments.of(1,getServerConfiguration(6),getLedgerLoggerProcessor(), true),
                Arguments.of(1,getServerConfiguration(7),getLedgerLoggerProcessor(), true),
                Arguments.of(1,getServerConfiguration(8),getLedgerLoggerProcessor(), true),

                //Ledger attivo non può avere id = 0
                Arguments.of(0,getServerConfiguration(1),getLedgerLoggerProcessor(), false),
                Arguments.of(0,getServerConfiguration(2),getLedgerLoggerProcessor(), true),
                Arguments.of(0,getServerConfiguration(3),getLedgerLoggerProcessor(), true),
                Arguments.of(0,getServerConfiguration(4),getLedgerLoggerProcessor(), true),
                Arguments.of(0,getServerConfiguration(5),getLedgerLoggerProcessor(), true),
                Arguments.of(0,getServerConfiguration(6),getLedgerLoggerProcessor(), true),
                Arguments.of(0,getServerConfiguration(7),getLedgerLoggerProcessor(), true),
                Arguments.of(0,getServerConfiguration(8),getLedgerLoggerProcessor(), true),

                Arguments.of(-1,getServerConfiguration(1),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,getServerConfiguration(2),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,getServerConfiguration(3),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,getServerConfiguration(4),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,getServerConfiguration(5),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,getServerConfiguration(6),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,getServerConfiguration(7),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,getServerConfiguration(8),getLedgerLoggerProcessor(), true),

                Arguments.of(1,getServerConfiguration(1),null, true),
                Arguments.of(1,getServerConfiguration(2),null, true),
                Arguments.of(1,getServerConfiguration(3),null, true),
                Arguments.of(1,getServerConfiguration(4),null, true),
                Arguments.of(1,getServerConfiguration(5),null, true),
                Arguments.of(1,getServerConfiguration(6),null, true),
                Arguments.of(1,getServerConfiguration(7),null, true),
                Arguments.of(1,getServerConfiguration(8),null, true),

                Arguments.of(0,getServerConfiguration(1),null, true),
                Arguments.of(0,getServerConfiguration(2),null, true),
                Arguments.of(0,getServerConfiguration(3),null, true),
                Arguments.of(0,getServerConfiguration(4),null, true),
                Arguments.of(0,getServerConfiguration(5),null, true),
                Arguments.of(0,getServerConfiguration(6),null, true),
                Arguments.of(0,getServerConfiguration(7),null, true),
                Arguments.of(0,getServerConfiguration(8),null, true),

                Arguments.of(-1,getServerConfiguration(1),null, true),
                Arguments.of(-1,getServerConfiguration(2),null, true),
                Arguments.of(-1,getServerConfiguration(3),null, true),
                Arguments.of(-1,getServerConfiguration(4),null, true),
                Arguments.of(-1,getServerConfiguration(5),null, true),
                Arguments.of(-1,getServerConfiguration(6),null, true),
                Arguments.of(-1,getServerConfiguration(7),null, true),
                Arguments.of(-1,getServerConfiguration(8),null, true)
        );
    }

    @ParameterizedTest
    @MethodSource("readLedgerIndexEntriesPartition")
    public void readLedgerIndexEntriesTest(long ledgerId, ServerConfiguration serverConf, SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor processor , boolean expectedException){
        try {
            BookieImpl bookie = new TestBookieImpl(serverConf);
            DbLedgerStorage thisStorage = (DbLedgerStorage) bookie.getLedgerStorage();

            thisStorage.start(); //Needed to start GC

            ByteBuf entry = Unpooled.buffer(BUFF_SIZE);
            entry.writeLong(ledgerId); // ledger id
            entry.writeLong(1); // entry id
            entry.writeBytes("entry-example-1".getBytes());
            thisStorage.addEntry(entry);

            assertEquals(entry,thisStorage.getEntry(ledgerId,1)); //Verify that just added entry is sotred in test DB

            thisStorage.flush(); //Update checkpoint

            ByteBuf entry2 = Unpooled.buffer(BUFF_SIZE);
            entry2.writeLong(ledgerId); // ledger id
            entry2.writeLong(2); // entry id
            entry2.writeBytes("entry-example-2".getBytes());

            thisStorage.addEntry(entry2);

            assertEquals(entry2, thisStorage.getEntry(ledgerId, 2));

            thisStorage.flush(); //Update checkpoint

            // Read last entry in ledger
            DbLedgerStorage.readLedgerIndexEntries(ledgerId,serverConf,processor);

            Assertions.assertFalse(expectedException);
        } catch (Exception e) {
            Assertions.assertTrue(expectedException);
        }

    }

    public static Stream<Arguments> addLedgerToIndexPartition() throws IOException, EmptyPagesException {
        return Stream.of(
                Arguments.of(1, true, "key".getBytes(), getPages(), false),
                Arguments.of(0, true, "key".getBytes(), getPages(), false),
                Arguments.of(-1, true, "key".getBytes(), getPages(), true),
                Arguments.of(1, false, "key".getBytes(), getPages(), false),
                Arguments.of(0, false, "key".getBytes(), getPages(), false),
                Arguments.of(-1, false, "key".getBytes(), getPages(), true),

                Arguments.of(1, true, "".getBytes(), getPages(), false),
                Arguments.of(0, true, "".getBytes(), getPages(), false),
                Arguments.of(-1, true, "".getBytes(), getPages(), true),
                Arguments.of(1, false, "".getBytes(), getPages(), false),
                Arguments.of(0, false, "".getBytes(), getPages(), false),
                Arguments.of(-1, false, "".getBytes(), getPages(), true),

                Arguments.of(1, true, null, getPages(), true),
                Arguments.of(0, true, null, getPages(), true),
                Arguments.of(-1, true, null, getPages(), true),
                Arguments.of(1, false, null, getPages(), true),
                Arguments.of(0, false, null, getPages(), true),
                Arguments.of(-1, false, null, getPages(), true),

                Arguments.of(1, true, "key".getBytes(), getEmptyPages(), true),
                Arguments.of(0, true, "key".getBytes(), getEmptyPages(), true),
                Arguments.of(-1, true, "key".getBytes(), getEmptyPages(), true),
                Arguments.of(1, false, "key".getBytes(), getEmptyPages(), true),
                Arguments.of(0, false, "key".getBytes(), getEmptyPages(), true),
                Arguments.of(-1, false, "key".getBytes(), getEmptyPages(), true),

                Arguments.of(1, true, "".getBytes(), getEmptyPages(), true),
                Arguments.of(0, true, "".getBytes(), getEmptyPages(), true),
                Arguments.of(-1, true, "".getBytes(), getEmptyPages(), true),
                Arguments.of(1, false, "".getBytes(), getEmptyPages(), true),
                Arguments.of(0, false, "".getBytes(), getEmptyPages(), true),
                Arguments.of(-1, false, "".getBytes(), getEmptyPages(), true),

                Arguments.of(1, true, null, getEmptyPages(), true),
                Arguments.of(0, true, null, getEmptyPages(), true),
                Arguments.of(-1, true, null, getEmptyPages(), true),
                Arguments.of(1, false, null, getEmptyPages(), true),
                Arguments.of(0, false, null, getEmptyPages(), true),
                Arguments.of(-1, false, null, getEmptyPages(), true),


                Arguments.of(1, true, "key".getBytes(), null, true),
                Arguments.of(0, true, "key".getBytes(), null, true),
                Arguments.of(-1, true, "key".getBytes(), null, true),
                Arguments.of(1, false, "key".getBytes(), null, true),
                Arguments.of(0, false, "key".getBytes(), null, true),
                Arguments.of(-1, false, "key".getBytes(), null, true),

                Arguments.of(1, true, "".getBytes(), null, true),
                Arguments.of(0, true, "".getBytes(), null, true),
                Arguments.of(-1, true, "".getBytes(), null, true),
                Arguments.of(1, false, "".getBytes(), null, true),
                Arguments.of(0, false, "".getBytes(), null, true),
                Arguments.of(-1, false, "".getBytes(), null, true),

                Arguments.of(1, true, null, null, true),
                Arguments.of(0, true, null, null, true),
                Arguments.of(-1, true, null, null, true),
                Arguments.of(1, false, null, null, true),
                Arguments.of(0, false, null, null, true),
                Arguments.of(-1, false, null, null, true)
        );
    }





    private static LedgerCache.PageEntriesIterable getPages() throws IOException {

        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();
        TestStatsProvider statsProvider = new TestStatsProvider();
        final long numWrites = 2000;
        final long entriesPerWrite = 2;
        final long numOfLedgers = 5;

        ServerConfiguration configuration = TestBKConfiguration.newServerConfiguration();
        configuration.setLedgerDirNames(new String[]{tmpDir.toString()});
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(configuration, configuration.getLedgerDirs(),
                new DiskChecker(configuration.getDiskUsageThreshold(), configuration.getDiskUsageWarnThreshold()));

        InterleavedLedgerStorageTest.TestableDefaultEntryLogger entryLogger = new InterleavedLedgerStorageTest.TestableDefaultEntryLogger(
                configuration, ledgerDirsManager, null, NullStatsLogger.INSTANCE);
        interleavedStorage.initializeWithEntryLogger(
                configuration, null, ledgerDirsManager, ledgerDirsManager,
                entryLogger, statsProvider.getStatsLogger(BOOKIE_SCOPE));
        interleavedStorage.setCheckpointer(Checkpointer.NULL);
        interleavedStorage.setCheckpointSource(CheckpointSource.DEFAULT);

        // Insert some ledger & entries in the interleaved storage
        for (long entryId = 0; entryId < numWrites; entryId++) {
            for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
                if (entryId == 0) {
                    interleavedStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
                    interleavedStorage.setFenced(ledgerId);
                }
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId * entriesPerWrite);
                entry.writeBytes(("entry-" + entryId).getBytes());

                interleavedStorage.addEntry(entry);
            }
        }
        return interleavedStorage.getIndexEntries(0);

    }


    private static LedgerCache.PageEntriesIterable getEmptyPages() throws EmptyPagesException {
        try {
            File tmpDir = File.createTempFile("bkTest", ".dir");
            tmpDir.delete();
            tmpDir.mkdir();
            File curDir = BookieImpl.getCurrentDirectory(tmpDir);
            BookieImpl.checkDirectoryStructure(curDir);
            final long numWrites = 0;
            final long entriesPerWrite = 1;
            final long numOfLedgers = 1;


            InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();
            TestStatsProvider statsProvider = new TestStatsProvider();


            ServerConfiguration configuration = TestBKConfiguration.newServerConfiguration();
            configuration.setLedgerDirNames(new String[]{tmpDir.toString()});
            LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(configuration, configuration.getLedgerDirs(),
                    new DiskChecker(configuration.getDiskUsageThreshold(), configuration.getDiskUsageWarnThreshold()));

            InterleavedLedgerStorageTest.TestableDefaultEntryLogger entryLogger = new InterleavedLedgerStorageTest.TestableDefaultEntryLogger(
                    configuration, ledgerDirsManager, null, NullStatsLogger.INSTANCE);

            interleavedStorage.initializeWithEntryLogger(
                    configuration, null, ledgerDirsManager, ledgerDirsManager,
                    entryLogger, statsProvider.getStatsLogger(BOOKIE_SCOPE));
            interleavedStorage.setCheckpointer(Checkpointer.NULL);
            interleavedStorage.setCheckpointSource(CheckpointSource.DEFAULT);

            for (long entryId = 0; entryId < numWrites; entryId++) {
                for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
                    if (entryId == 0) {
                        interleavedStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
                        interleavedStorage.setFenced(ledgerId);
                    }
                    ByteBuf entry = Unpooled.buffer(128);
                    entry.writeLong(ledgerId);
                    entry.writeLong(entryId * entriesPerWrite);
                    entry.writeBytes(("entry-" + entryId).getBytes());

                    interleavedStorage.addEntry(entry);
                }
            }

            return interleavedStorage.getIndexEntries(0);
        }catch (IOException e){
            return null; //Check how to set better
        }
    }


    @ParameterizedTest
    @MethodSource("addLedgerToIndexPartition") //master key is used for crypto reasons
    public void addLedgerToIndex(long ledgerId, boolean isFenced, byte[] masterKey,
                                 LedgerCache.PageEntriesIterable pages, boolean expectedException) {
        //Fanced means read only
        try {
            storage.setMasterKey(ledgerId, masterKey);


            ByteBuf entry = Unpooled.buffer(BUFF_SIZE);
            entry.writeLong(ledgerId); // ledger id

            //Populate ledger
            for (int i = 0; i < 100; i++) {
                entry.writeLong(i); // entry id
                entry.writeBytes(("entry" + i).getBytes());
                storage.addEntry(entry);
            }
            storage.flush();

            // Simulate bookie compaction
            SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
            singleDirStorage.addLedgerToIndex(ledgerId, isFenced, masterKey, pages);
            singleDirStorage.flush();
            Assertions.assertTrue(singleDirStorage.ledgerExists(ledgerId));

            Assertions.assertFalse(expectedException);
        } catch (Exception e) {
            if(!e.getClass().getName().equals(EmptyPagesException.class.toString()))
                Assertions.assertTrue(expectedException);
        }

    }

    @Test
    public void testBookieCompaction() throws Exception {
        storage.setMasterKey(4, "key".getBytes());

        ByteBuf entry3 = Unpooled.buffer(BUFF_SIZE);
        entry3.writeLong(4); // ledger id
        entry3.writeLong(3); // entry id
        entry3.writeBytes("entry-3".getBytes());
        storage.addEntry(entry3);


        // Simulate bookie compaction
        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
        EntryLogger entryLogger = singleDirStorage.getEntryLogger();
        // Rewrite entry-3
        ByteBuf newEntry3 = Unpooled.buffer(BUFF_SIZE);
        newEntry3.writeLong(4); // ledger id
        newEntry3.writeLong(3); // entry id
        newEntry3.writeBytes("new-entry-3".getBytes());
        long location = entryLogger.addEntry(4L, newEntry3);
        newEntry3.resetReaderIndex();

        storage.flush();
        List<EntryLocation> locations = Lists.newArrayList(new EntryLocation(4, 3, location));
        singleDirStorage.updateEntriesLocations(locations);

        ByteBuf res = storage.getEntry(4, 3);
        assertEquals(newEntry3, res);
    }


    public static Stream<Arguments> doGetEntryPartition() {
        return Stream.of(
                Arguments.of(1,-1,true),
                Arguments.of(1,1,false),
                Arguments.of(1,0,false),

                Arguments.of(0,-1,true),
                Arguments.of(0,1,false),
                Arguments.of(0,0,false),

                Arguments.of(-1,-1,true),
                Arguments.of(-1,1,true),
                Arguments.of(-1,0,true)
        );
    }


    //Implement flush caching logic to upgrade coverage
    @ParameterizedTest
    @MethodSource("doGetEntryPartition")
    public void doGetEntryTest(long ledgerId, long entryId, boolean exceptionExpected){
        try {
            storage.setMasterKey(ledgerId, "key".getBytes());
            ByteBuf entry = Unpooled.buffer(BUFF_SIZE);
            entry.writeLong(ledgerId); // ledger id
            entry.writeLong(entryId); // entry id
            entry.writeBytes("entry".getBytes());
            storage.addEntry(entry);



            Assertions.assertEquals(storage.getEntry(ledgerId,BookieProtocol.LAST_ADD_CONFIRMED),storage.getLastEntry(ledgerId)); //Check if entry has been confirmed

            // Simulate bookie compaction
            SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);



            Assertions.assertTrue(singleDirStorage.ledgerExists(ledgerId));

//            //Enable flushing loop
//            singleDirStorage.writeCacheBeingFlushed = new WriteCache(new UnpooledByteBufAllocator(true)BUFF_SIZE);
//            singleDirStorage.writeCache = new WriteCache(new UnpooledByteBufAllocator(true)BUFF_SIZE);
//

            ByteBuf storageEntry = singleDirStorage.doGetEntry(ledgerId,entryId);

            singleDirStorage.doGetEntry(ledgerId,entryId);
            Assertions.assertEquals(entry,storageEntry);

            Assertions.assertFalse(exceptionExpected);
        } catch (Exception e) {
            Assertions.assertTrue(exceptionExpected);
        }


    }
}
