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
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.bookie.DefaultEntryLogger.*;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.After;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
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

    public DbLedgerStorageTest(){
        try {
            setup();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void setup() throws Exception {
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

    @After
    public void teardown() throws Exception {
        storage.shutdown();
        tmpDir.delete();
    }


    public static Stream<Arguments> addEntryPartition() {
        return Stream.of(
                Arguments.of(null, true),
                Arguments.of(Unpooled.buffer(1024),false),
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
            Assertions.assertTrue(true);
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
            ByteBuf entry = Unpooled.buffer(1024);
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

            ByteBuf entry = Unpooled.buffer(1024);
            entry.writeLong(ledgerId); // ledger id
            entry.writeLong(1); // entry id
            entry.writeBytes("entry-example-1".getBytes());
            thisStorage.addEntry(entry);

            assertEquals(entry,thisStorage.getEntry(ledgerId,1)); //Verify that just added entry is sotred in test DB

            thisStorage.flush(); //Update checkpoint

            ByteBuf entry2 = Unpooled.buffer(1024);
            entry2.writeLong(ledgerId); // ledger id
            entry2.writeLong(2); // entry id
            entry2.writeBytes("entry-example-2".getBytes());

            thisStorage.addEntry(entry2);

            assertEquals(entry2, thisStorage.getEntry(ledgerId, 2));

            thisStorage.flush(); //Update checkpoint


            System.out.println("Ledger id:" + ledgerId);

            // Read last entry in ledger
            DbLedgerStorage.readLedgerIndexEntries(ledgerId,serverConf,processor);

            Assertions.assertFalse(expectedException);
        } catch (Exception e) {
            Assertions.assertTrue(expectedException);
        }

    }

//
//    @Test
//    public void simple() throws Exception {
//        assertEquals(false, storage.ledgerExists(3));
//        try {
//            storage.isFenced(3);
//            fail("should have failed");
//        } catch (Bookie.NoLedgerException nle) {
//            // OK
//        }
//        assertEquals(false, storage.ledgerExists(3));
//        try {
//            storage.setFenced(3);
//            fail("should have failed");
//        } catch (Bookie.NoLedgerException nle) {
//            // OK
//        }
//        storage.setMasterKey(3, "key".getBytes());
//        try {
//            storage.setMasterKey(3, "other-key".getBytes());
//            fail("should have failed");
//        } catch (IOException ioe) {
//            assertTrue(ioe.getCause() instanceof BookieException.BookieIllegalOpException);
//        }
//        // setting the same key is NOOP
//        storage.setMasterKey(3, "key".getBytes());
//        assertEquals(true, storage.ledgerExists(3));
//        assertEquals(true, storage.setFenced(3));
//        assertEquals(true, storage.isFenced(3));
//        assertEquals(false, storage.setFenced(3));
//
//        storage.setMasterKey(4, "key".getBytes());
//        assertEquals(false, storage.isFenced(4));
//        assertEquals(true, storage.ledgerExists(4));
//
//        assertEquals("key", new String(storage.readMasterKey(4)));
//
//        assertEquals(Lists.newArrayList(4L, 3L), Lists.newArrayList(storage.getActiveLedgersInRange(0, 100)));
//        assertEquals(Lists.newArrayList(4L, 3L), Lists.newArrayList(storage.getActiveLedgersInRange(3, 100)));
//        assertEquals(Lists.newArrayList(3L), Lists.newArrayList(storage.getActiveLedgersInRange(0, 4)));
//
//        // Add / read entries
//        ByteBuf entry = Unpooled.buffer(1024);
//        entry.writeLong(4); // ledger id
//        entry.writeLong(1); // entry id
//        entry.writeLong(0); // lac
//        entry.writeBytes("entry-1".getBytes());
//
//        assertEquals(false, ((DbLedgerStorage) storage).isFlushRequired());
//
//        assertEquals(1, storage.addEntry(entry));
//
//        assertEquals(true, ((DbLedgerStorage) storage).isFlushRequired());
//
//        // Read from write cache
//        assertTrue(storage.entryExists(4, 1));
//        ByteBuf res = storage.getEntry(4, 1);
//        assertEquals(entry, res);
//
//        storage.flush();
//
//        assertEquals(false, ((DbLedgerStorage) storage).isFlushRequired());
//
//        // Read from db
//        assertTrue(storage.entryExists(4, 1));
//        res = storage.getEntry(4, 1);
//        assertEquals(entry, res);
//
//        try {
//            storage.getEntry(4, 2);
//            fail("Should have thrown exception");
//        } catch (NoEntryException e) {
//            // ok
//        }
//
//        ByteBuf entry2 = Unpooled.buffer(1024);
//        entry2.writeLong(4); // ledger id
//        entry2.writeLong(2); // entry id
//        entry2.writeLong(1); // lac
//        entry2.writeBytes("entry-2".getBytes());
//
//        storage.addEntry(entry2);
//
//        // Read last entry in ledger
//        res = storage.getEntry(4, BookieProtocol.LAST_ADD_CONFIRMED);
//        assertEquals(entry2, res);
//
//        // Read last add confirmed in ledger
//        assertEquals(1L, storage.getLastAddConfirmed(4));
//
//        ByteBuf entry3 = Unpooled.buffer(1024);
//        entry3.writeLong(4); // ledger id
//        entry3.writeLong(3); // entry id
//        entry3.writeLong(2); // lac
//        entry3.writeBytes("entry-3".getBytes());
//        storage.addEntry(entry3);
//
//        ByteBuf entry4 = Unpooled.buffer(1024);
//        entry4.writeLong(4); // ledger id
//        entry4.writeLong(4); // entry id
//        entry4.writeLong(3); // lac
//        entry4.writeBytes("entry-4".getBytes());
//        storage.addEntry(entry4);
//
//        res = storage.getEntry(4, 4);
//        assertEquals(entry4, res);
//
//        assertEquals(3, storage.getLastAddConfirmed(4));
//
//        // Delete
//        assertEquals(true, storage.ledgerExists(4));
//        storage.deleteLedger(4);
//        assertEquals(false, storage.ledgerExists(4));
//
//        // remove entries for ledger 4 from cache
//        storage.flush();
//
//        try {
//            storage.getEntry(4, 4);
//            fail("Should have thrown exception since the ledger was deleted");
//        } catch (Bookie.NoLedgerException e) {
//            // ok
//        }
//    }
//

    public static Stream<Arguments> addLedgerToIndexPartition(){
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





    private static LedgerCache.PageEntriesIterable getPages() {
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    private static LedgerCache.PageEntriesIterable getEmptyPages() {
        try {
            File tmpDir = File.createTempFile("bkTest", ".dir");
            tmpDir.delete();
            tmpDir.mkdir();
            File curDir = BookieImpl.getCurrentDirectory(tmpDir);
            BookieImpl.checkDirectoryStructure(curDir);

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


            return interleavedStorage.getIndexEntries(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    @ParameterizedTest
    @MethodSource("addLedgerToIndexPartition") //master key is used for crypto reasons
    public void addLedgerToIndex(long ledgerId, boolean isFenced, byte[] masterKey,
                                 LedgerCache.PageEntriesIterable pages ,boolean expectedException){
        //Fanced means read only
        try {
            storage.setMasterKey(ledgerId, masterKey);


            ByteBuf entry = Unpooled.buffer(1024);
            entry.writeLong(ledgerId); // ledger id

            //Populate ledger
            for(int i = 0;i<100;i++){
                entry.writeLong(i); // entry id
                entry.writeBytes(("entry" + i ).getBytes());
                storage.addEntry(entry);
            }
            storage.flush();

            // Simulate bookie compaction
            SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
            singleDirStorage.addLedgerToIndex(ledgerId,isFenced,masterKey,pages);
            singleDirStorage.flush();
            Assertions.assertTrue(singleDirStorage.ledgerExists(ledgerId));

            Assertions.assertFalse(expectedException);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.assertTrue(expectedException);
        }

    }

    @Test
    public void testBookieCompaction() throws Exception {
        storage.setMasterKey(4, "key".getBytes());

        ByteBuf entry3 = Unpooled.buffer(1024);
        entry3.writeLong(4); // ledger id
        entry3.writeLong(3); // entry id
        entry3.writeBytes("entry-3".getBytes());
        storage.addEntry(entry3);


        // Simulate bookie compaction
        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
        EntryLogger entryLogger = singleDirStorage.getEntryLogger();
        // Rewrite entry-3
        ByteBuf newEntry3 = Unpooled.buffer(1024);
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




//
//    @Test
//    public void doubleDirectory() throws Exception {
//        int gcWaitTime = 1000;
//        File firstDir = new File(tmpDir, "dir1");
//        File secondDir = new File(tmpDir, "dir2");
//        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
//        conf.setGcWaitTime(gcWaitTime);
//        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 4);
//        conf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 4);
//        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
//        conf.setLedgerDirNames(new String[] { firstDir.getCanonicalPath(), secondDir.getCanonicalPath() });
//
//        // Should not fail
//        Bookie bookie = new TestBookieImpl(conf);
//        assertEquals(2, ((DbLedgerStorage) bookie.getLedgerStorage()).getLedgerStorageList().size());
//
//        bookie.shutdown();
//    }
//
//    @Test
//    public void testRewritingEntries() throws Exception {
//        storage.setMasterKey(1, "key".getBytes());
//
//        try {
//            storage.getEntry(1, -1);
//            fail("Should throw exception");
//        } catch (NoEntryException e) {
//            // ok
//        }
//
//        ByteBuf entry1 = Unpooled.buffer(1024);
//        entry1.writeLong(1); // ledger id
//        entry1.writeLong(1); // entry id
//        entry1.writeBytes("entry-1".getBytes());
//
//        storage.addEntry(entry1);
//        storage.flush();
//
//        ByteBuf newEntry1 = Unpooled.buffer(1024);
//        newEntry1.writeLong(1); // ledger id
//        newEntry1.writeLong(1); // entry id
//        newEntry1.writeBytes("new-entry-1".getBytes());
//
//        storage.addEntry(newEntry1);
//        storage.flush();
//
//        ByteBuf response = storage.getEntry(1, 1);
//        assertEquals(newEntry1, response);
//    }
//
//    @Test
//    public void testEntriesOutOfOrder() throws Exception {
//        storage.setMasterKey(1, "key".getBytes());
//
//        ByteBuf entry2 = Unpooled.buffer(1024);
//        entry2.writeLong(1); // ledger id
//        entry2.writeLong(2); // entry id
//        entry2.writeBytes("entry-2".getBytes());
//
//        storage.addEntry(entry2);
//
//        try {
//            storage.getEntry(1, 1);
//            fail("Entry doesn't exist");
//        } catch (NoEntryException e) {
//            // Ok, entry doesn't exist
//        }
//
//        ByteBuf res = storage.getEntry(1, 2);
//        assertEquals(entry2, res);
//
//        ByteBuf entry1 = Unpooled.buffer(1024);
//        entry1.writeLong(1); // ledger id
//        entry1.writeLong(1); // entry id
//        entry1.writeBytes("entry-1".getBytes());
//
//        storage.addEntry(entry1);
//
//        res = storage.getEntry(1, 1);
//        assertEquals(entry1, res);
//
//        res = storage.getEntry(1, 2);
//        assertEquals(entry2, res);
//
//        storage.flush();
//
//        res = storage.getEntry(1, 1);
//        assertEquals(entry1, res);
//
//        res = storage.getEntry(1, 2);
//        assertEquals(entry2, res);
//    }
//
//    @Test
//    public void testEntriesOutOfOrderWithFlush() throws Exception {
//        storage.setMasterKey(1, "key".getBytes());
//
//        ByteBuf entry2 = Unpooled.buffer(1024);
//        entry2.writeLong(1); // ledger id
//        entry2.writeLong(2); // entry id
//        entry2.writeBytes("entry-2".getBytes());
//
//        storage.addEntry(entry2);
//
//        try {
//            storage.getEntry(1, 1);
//            fail("Entry doesn't exist");
//        } catch (NoEntryException e) {
//            // Ok, entry doesn't exist
//        }
//
//        ByteBuf res = storage.getEntry(1, 2);
//        assertEquals(entry2, res);
//        ReferenceCountUtil.release(res);
//
//        storage.flush();
//
//        try {
//            storage.getEntry(1, 1);
//            fail("Entry doesn't exist");
//        } catch (NoEntryException e) {
//            // Ok, entry doesn't exist
//        }
//
//        res = storage.getEntry(1, 2);
//        assertEquals(entry2, res);
//        ReferenceCountUtil.release(res);
//
//        ByteBuf entry1 = Unpooled.buffer(1024);
//        entry1.writeLong(1); // ledger id
//        entry1.writeLong(1); // entry id
//        entry1.writeBytes("entry-1".getBytes());
//
//        storage.addEntry(entry1);
//
//        res = storage.getEntry(1, 1);
//        assertEquals(entry1, res);
//        ReferenceCountUtil.release(res);
//
//        res = storage.getEntry(1, 2);
//        assertEquals(entry2, res);
//        ReferenceCountUtil.release(res);
//
//        storage.flush();
//
//        res = storage.getEntry(1, 1);
//        assertEquals(entry1, res);
//        ReferenceCountUtil.release(res);
//
//        res = storage.getEntry(1, 2);
//        assertEquals(entry2, res);
//        ReferenceCountUtil.release(res);
//    }
//
//    @Test
//    public void testAddEntriesAfterDelete() throws Exception {
//        storage.setMasterKey(1, "key".getBytes());
//
//        ByteBuf entry0 = Unpooled.buffer(1024);
//        entry0.writeLong(1); // ledger id
//        entry0.writeLong(0); // entry id
//        entry0.writeBytes("entry-0".getBytes());
//
//        ByteBuf entry1 = Unpooled.buffer(1024);
//        entry1.writeLong(1); // ledger id
//        entry1.writeLong(1); // entry id
//        entry1.writeBytes("entry-1".getBytes());
//
//        storage.addEntry(entry0);
//        storage.addEntry(entry1);
//
//        storage.flush();
//
//        storage.deleteLedger(1);
//
//        storage.setMasterKey(1, "key".getBytes());
//
//        entry0 = Unpooled.buffer(1024);
//        entry0.writeLong(1); // ledger id
//        entry0.writeLong(0); // entry id
//        entry0.writeBytes("entry-0".getBytes());
//
//        entry1 = Unpooled.buffer(1024);
//        entry1.writeLong(1); // ledger id
//        entry1.writeLong(1); // entry id
//        entry1.writeBytes("entry-1".getBytes());
//
//        storage.addEntry(entry0);
//        storage.addEntry(entry1);
//
//        assertEquals(entry0, storage.getEntry(1, 0));
//        assertEquals(entry1, storage.getEntry(1, 1));
//
//        storage.flush();
//    }
//
//    @Test
//    public void testLimboStateSucceedsWhenInLimboButHasEntry() throws Exception {
//        storage.setMasterKey(1, "foobar".getBytes());
//
//        ByteBuf entry0 = Unpooled.buffer(1024);
//        entry0.writeLong(1); // ledger id
//        entry0.writeLong(0); // entry id
//        entry0.writeBytes("entry-0".getBytes());
//
//        storage.addEntry(entry0);
//        storage.flush();
//        storage.setLimboState(1);
//
//        try {
//            storage.getEntry(1, 0);
//        } catch (BookieException.DataUnknownException e) {
//            fail("Should have been able to read entry");
//        }
//    }
//
//    @Test
//    public void testLimboStateThrowsInLimboWhenNoEntry() throws Exception {
//        storage.setMasterKey(1, "foobar".getBytes());
//
//        ByteBuf entry0 = Unpooled.buffer(1024);
//        entry0.writeLong(1); // ledger id
//        entry0.writeLong(1); // entry id
//        entry0.writeBytes("entry-0".getBytes());
//
//        storage.addEntry(entry0);
//        storage.flush();
//        storage.setLimboState(1);
//
//        try {
//            storage.getEntry(1, 1);
//        } catch (NoEntryException nee) {
//            fail("Shouldn't have seen NoEntryException");
//        } catch (BookieException.DataUnknownException e) {
//            // expected
//        }
//
//        storage.shutdown();
//        Bookie restartedBookie = new TestBookieImpl(conf);
//        DbLedgerStorage restartedStorage = (DbLedgerStorage) restartedBookie.getLedgerStorage();
//        try {
//            try {
//                restartedStorage.getEntry(1, 1);
//            } catch (NoEntryException nee) {
//                fail("Shouldn't have seen NoEntryException");
//            } catch (BookieException.DataUnknownException e) {
//                // expected
//            }
//        } finally {
//            restartedStorage.shutdown();
//        }
//
//        storage = (DbLedgerStorage) new TestBookieImpl(conf).getLedgerStorage();
//    }
//
//    @Test
//    public void testLimboStateThrowsNoEntryExceptionWhenLimboCleared() throws Exception {
//        storage.setMasterKey(1, "foobar".getBytes());
//
//        ByteBuf entry0 = Unpooled.buffer(1024);
//        entry0.writeLong(1); // ledger id
//        entry0.writeLong(1); // entry id
//        entry0.writeBytes("entry-0".getBytes());
//
//        storage.addEntry(entry0);
//        storage.flush();
//        storage.setLimboState(1);
//
//        try {
//            storage.getEntry(1, 1);
//        } catch (NoEntryException nee) {
//            fail("Shouldn't have seen NoEntryException");
//        } catch (BookieException.DataUnknownException e) {
//            // expected
//        }
//
//        storage.clearLimboState(1);
//        try {
//            storage.getEntry(1, 1);
//        } catch (NoEntryException nee) {
//            // expected
//        } catch (BookieException.DataUnknownException e) {
//            fail("Should have seen NoEntryException");
//        }
//    }
//
//    @Test
//    public void testLimboStateSucceedsWhenFenced() throws Exception {
//        storage.setMasterKey(1, "foobar".getBytes());
//
//        ByteBuf entry0 = Unpooled.buffer(1024);
//        entry0.writeLong(1); // ledger id
//        entry0.writeLong(1); // entry id
//        entry0.writeBytes("entry-0".getBytes());
//
//        storage.addEntry(entry0);
//        storage.flush();
//        storage.setFenced(1);
//        storage.setLimboState(1);
//
//        try {
//            storage.isFenced(1);
//        } catch (IOException ioe) {
//            fail("Should have been able to get isFenced response");
//        }
//    }
//
//    @Test
//    public void testLimboStateThrowsInLimboWhenNotFenced() throws Exception {
//        storage.setMasterKey(1, "foobar".getBytes());
//
//        ByteBuf entry0 = Unpooled.buffer(1024);
//        entry0.writeLong(1); // ledger id
//        entry0.writeLong(1); // entry id
//        entry0.writeBytes("entry-0".getBytes());
//
//        storage.addEntry(entry0);
//        storage.flush();
//        storage.setLimboState(1);
//
//        try {
//            storage.isFenced(1);
//            fail("Shouldn't have been able to get isFenced response");
//        } catch (BookieException.DataUnknownException e) {
//            // expected
//        }
//    }
//
//    @Test
//    public void testHasEntry() throws Exception {
//        long ledgerId = 0xbeefee;
//        storage.setMasterKey(ledgerId, "foobar".getBytes());
//
//        ByteBuf entry0 = Unpooled.buffer(1024);
//        entry0.writeLong(ledgerId); // ledger id
//        entry0.writeLong(0); // entry id
//        entry0.writeBytes("entry-0".getBytes());
//
//        storage.addEntry(entry0);
//
//        // should come from write cache
//        assertTrue(storage.entryExists(ledgerId, 0));
//        assertFalse(storage.entryExists(ledgerId, 1));
//
//        storage.flush();
//
//        // should come from storage
//        assertTrue(storage.entryExists(ledgerId, 0));
//        assertFalse(storage.entryExists(ledgerId, 1));
//
//        // pull entry into readcache
//        storage.getEntry(ledgerId, 0);
//
//        // should come from read cache
//        assertTrue(storage.entryExists(ledgerId, 0));
//        assertFalse(storage.entryExists(ledgerId, 1));
//    }
//
//    @Test
//    public void testStorageStateFlags() throws Exception {
//        assertTrue(storage.getStorageStateFlags().isEmpty());
//
//        storage.setStorageStateFlag(LedgerStorage.StorageState.NEEDS_INTEGRITY_CHECK);
//        assertTrue(storage.getStorageStateFlags()
//                   .contains(LedgerStorage.StorageState.NEEDS_INTEGRITY_CHECK));
//
//        storage.shutdown();
//        Bookie restartedBookie1 = new TestBookieImpl(conf);
//        DbLedgerStorage restartedStorage1 = (DbLedgerStorage) restartedBookie1.getLedgerStorage();
//        try {
//            assertTrue(restartedStorage1.getStorageStateFlags()
//                   .contains(LedgerStorage.StorageState.NEEDS_INTEGRITY_CHECK));
//            restartedStorage1.clearStorageStateFlag(LedgerStorage.StorageState.NEEDS_INTEGRITY_CHECK);
//
//            assertFalse(restartedStorage1.getStorageStateFlags()
//                   .contains(LedgerStorage.StorageState.NEEDS_INTEGRITY_CHECK));
//
//        } finally {
//            restartedStorage1.shutdown();
//        }
//
//        Bookie restartedBookie2 = new TestBookieImpl(conf);
//        DbLedgerStorage restartedStorage2 = (DbLedgerStorage) restartedBookie2.getLedgerStorage();
//        try {
//            assertFalse(restartedStorage2.getStorageStateFlags()
//                   .contains(LedgerStorage.StorageState.NEEDS_INTEGRITY_CHECK));
//        } finally {
//            restartedStorage2.shutdown();
//        }
//
//        storage = (DbLedgerStorage) new TestBookieImpl(conf).getLedgerStorage();
//    }
}
