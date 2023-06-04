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

import com.sun.org.apache.xpath.internal.Arg;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.After;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;


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

        int gcWaitTime = 1000;
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
            Assertions.assertEquals(entry,storage.getEntry(4,1)); //Verify that just added entry is sotred in test DB
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
                Arguments.of(-1,1, true),
                Arguments.of(1,-1, true),
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
            Assertions.assertEquals(entry,storage.getEntry(ledgerId,entryId)); //Verify that just added entry is sotred in test DB
            Assertions.assertFalse(expectedException);
        } catch (Exception e) {
            Assertions.assertTrue(expectedException);
        }

    }


    private static ServerConfiguration getServerConfiguration(int testCase) throws IOException {
        int gcWaitTime;
        String ledgerStorageClass;
        ServerConfiguration serverConfiguration = TestBKConfiguration.newServerConfiguration();
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
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

        serverConfiguration.setGcWaitTime(gcWaitTime);
        serverConfiguration.setLedgerStorageClass(ledgerStorageClass); //Set this class as the persistence one
        serverConfiguration.setLedgerDirNames(new String[] { tmpDir.toString() });
        return serverConfiguration;
    }


    private static SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor getLedgerLoggerProcessor() {
        return (currentEntry, entryLodId, position) -> System.out
                .println("entry " + currentEntry + "\t:\t(log: " + entryLodId + ", pos: " + position + ")");
    }



//    public static Stream<Arguments> readLedgerIndexEntriesPartition() throws IOException {
//        return Stream.of(
//                Arguments.of(1,getServerConfiguration(1),getLedgerLoggerProcessor(), false),
//                Arguments.of(1,getServerConfiguration(2),getLedgerLoggerProcessor(), true),
//                Arguments.of(1,getServerConfiguration(3),getLedgerLoggerProcessor(), true),
//                Arguments.of(1,getServerConfiguration(4),getLedgerLoggerProcessor(), true),
//                Arguments.of(1,getServerConfiguration(5),getLedgerLoggerProcessor(), true),
//                Arguments.of(1,getServerConfiguration(6),getLedgerLoggerProcessor(), true),
//                Arguments.of(1,getServerConfiguration(7),getLedgerLoggerProcessor(), true),
//                Arguments.of(1,getServerConfiguration(8),getLedgerLoggerProcessor(), true),
//                Arguments.of(-1,getServerConfiguration(1),getLedgerLoggerProcessor(), true),
//                Arguments.of(-1,getServerConfiguration(2),getLedgerLoggerProcessor(), true),
//                Arguments.of(-1,getServerConfiguration(3),getLedgerLoggerProcessor(), true),
//                Arguments.of(-1,getServerConfiguration(4),getLedgerLoggerProcessor(), true),
//                Arguments.of(-1,getServerConfiguration(5),getLedgerLoggerProcessor(), true),
//                Arguments.of(-1,getServerConfiguration(6),getLedgerLoggerProcessor(), true),
//                Arguments.of(-1,getServerConfiguration(7),getLedgerLoggerProcessor(), true),
//                Arguments.of(-1,getServerConfiguration(8),getLedgerLoggerProcessor(), true)
//        );
//    }
//
//    @ParameterizedTest
//    @MethodSource("readLedgerIndexEntriesPartition")
//    public void readLedgerIndexEntriesTest(long ledgerId, ServerConfiguration serverConf, SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor processor , boolean expectedException){
//        try {
//            BookieImpl bookie = new TestBookieImpl(serverConf);
//
//            DbLedgerStorage.readLedgerIndexEntries(ledgerId,serverConf,processor);
//        } catch (Exception e) {
//            Assertions.assertTrue(expectedException);
//        }
//
//    }

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
//    @Test
//    public void testBookieCompaction() throws Exception {
//        storage.setMasterKey(4, "key".getBytes());
//
//        ByteBuf entry3 = Unpooled.buffer(1024);
//        entry3.writeLong(4); // ledger id
//        entry3.writeLong(3); // entry id
//        entry3.writeBytes("entry-3".getBytes());
//        storage.addEntry(entry3);
//
//
//        // Simulate bookie compaction
//        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
//        EntryLogger entryLogger = singleDirStorage.getEntryLogger();
//        // Rewrite entry-3
//        ByteBuf newEntry3 = Unpooled.buffer(1024);
//        newEntry3.writeLong(4); // ledger id
//        newEntry3.writeLong(3); // entry id
//        newEntry3.writeBytes("new-entry-3".getBytes());
//        long location = entryLogger.addEntry(4L, newEntry3);
//        newEntry3.resetReaderIndex();
//
//        storage.flush();
//        List<EntryLocation> locations = Lists.newArrayList(new EntryLocation(4, 3, location));
//        singleDirStorage.updateEntriesLocations(locations);
//
//        ByteBuf res = storage.getEntry(4, 3);
//        System.out.println("res:       " + ByteBufUtil.hexDump(res));
//        System.out.println("newEntry3: " + ByteBufUtil.hexDump(newEntry3));
//        assertEquals(newEntry3, res);
//    }
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
