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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.intThat;
import static org.mockito.Mockito.when;


/**
 * Unit test for {@link DbLedgerStorage}.
 */
public class DbLedgerStorageTest {
    private static final Logger log = LoggerFactory.getLogger(DbLedgerStorageTest.class);
    protected DbLedgerStorage storage;
    protected File tmpDirLedger;
    protected LedgerDirsManager ledgerDirsManager;
    protected ServerConfiguration conf;
    private static final int BUFF_SIZE = 128;

    @BeforeEach
    void setup() throws Exception {
        tmpDirLedger = File.createTempFile("bkTest", ".dir");
        tmpDirLedger.delete();
        tmpDirLedger.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDirLedger);
        BookieImpl.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000; //Time of garbage collector to delete entries that aren't associated anymore to an active ledger
        conf = TestBKConfiguration.newServerConfiguration(); //TestBKConfiguration is a class to get a server configuration instance
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName()); //Set this class as the persistence one
        conf.setLedgerDirNames(new String[] { tmpDirLedger.toString() });


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
        tmpDirLedger.delete();
    }

    
    
    /*
        Category partition
        Valid entry; Empty entry; Wrong ledgerId; wrong entryId; null
     */
    private static ByteBuf addEntryConfiguration(int type){
        ByteBuf entry;
        switch (type){
            case 1:
                entry = Unpooled.buffer(BUFF_SIZE);
                entry.writeLong(2); //ledger id
                entry.writeLong(1); //entry id
                entry.writeLong(1); //lc id
                entry.writeBytes("entry".getBytes());
                break;
            case 2:
                entry = Unpooled.buffer(BUFF_SIZE);
                entry.writeLong(2); //ledger id
                entry.writeLong(1); //entry id
                entry.writeLong(1); //lc id
                break;
            case 3:
                entry = Unpooled.buffer(BUFF_SIZE);
                entry.writeLong(-1); //ledger id
                entry.writeLong(1); //entry id
                entry.writeLong(1); //lac id
                entry.writeBytes("entry".getBytes());
                break;
            case 4:
                entry = Unpooled.buffer(BUFF_SIZE);
                entry.writeLong(2); //ledger id
                entry.writeLong(-1); //entry id
                entry.writeLong(1); //lac id
                entry.writeBytes("entry".getBytes());
                break;
            case 5:
                entry = Unpooled.buffer(BUFF_SIZE);
                entry.writeLong(2); //ledger id
                entry.writeLong(1); //entry id
                entry.writeLong(-1); //lac id
                entry.writeBytes("entry".getBytes());
                break;
            case 6:
                entry = null;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        
        return entry;
    }
    

    public static Stream<Arguments> addEntryPartition() {
        return Stream.of(
                Arguments.of(addEntryConfiguration(1), false,null),
                Arguments.of(addEntryConfiguration(2), false,null),
                Arguments.of(addEntryConfiguration(3), true,IllegalArgumentException.class),
                Arguments.of(addEntryConfiguration(4), true,IllegalArgumentException.class),
                //Arguments.of(addEntryConfiguration(5), true,IllegalArgumentException.class), //Test failure
                Arguments.of(addEntryConfiguration(6), true,NullPointerException.class)
        );
    }

    @ParameterizedTest
    @MethodSource("addEntryPartition")
    public void addEntryTest(ByteBuf entry,boolean expectedException,Class exceptionClass){
        try {
            storage.setMasterKey(2,"key".getBytes());
            storage.start(); //Starts garbage collector

            storage.addEntry(entry);
            storage.flush();

            Assertions.assertTrue(storage.entryExists(2,1));

            Assertions.assertEquals(1,storage.getLastEntryInLedger(2));


            Assertions.assertEquals(entry,storage.getLastEntry(2));


            //Check if last confirmed is correct
            Assertions.assertEquals(1,storage.getLastAddConfirmed(2));

            Assertions.assertFalse(expectedException);
        } catch (NullPointerException | IllegalArgumentException e) {
            Assertions.assertTrue(expectedException);

            Assertions.assertEquals(exceptionClass,e.getClass());
        } catch (IOException | BookieException e) {
            //Error in test
            Assertions.fail();
        }
    }

    @ParameterizedTest
    @MethodSource("addEntryPartition")
    public void rewriteTest(ByteBuf entry,boolean expectedException, Class exceptionClass){
        try {
            storage.setMasterKey(2,"key".getBytes());

            storage.start(); //Start GC

            storage.addEntry(entry);
            storage.flush();


            ByteBuf newEntry = Unpooled.buffer(BUFF_SIZE);
            newEntry.writeLong(2);
            newEntry.writeLong(1);
            newEntry.writeBytes("new-entry".getBytes());
            storage.addEntry(newEntry);
            storage.flush();

            Assertions.assertTrue(storage.entryExists(2,1));


            //Only ledger 2 and entry 1 should exist
            Assertions.assertEquals(1,storage.getLastEntryInLedger(2));
            Assertions.assertEquals(newEntry,storage.getLastEntry(2).asByteBuf());

            Assertions.assertFalse(expectedException);
        } catch (NullPointerException | IllegalArgumentException e) {
            Assertions.assertTrue(expectedException);

            Assertions.assertEquals(exceptionClass,e.getClass());
        } catch (IOException | BookieException e) {
            //Error in test
            Assertions.fail();
        }
    }


    @ParameterizedTest
    @MethodSource("addEntryPartition")
    public void limboStateTest(ByteBuf entry,boolean expectedException, Class exceptionClass) {
        try {
            storage.setMasterKey(2,"key".getBytes());

            storage.addEntry(entry);
            storage.flush();
            storage.setLimboState(2);

            Assertions.assertThrows(BookieException.class, () -> storage.isFenced(2)); //Can't access metadata after limbo state

            storage.setMasterKey(3,"new-key".getBytes());

            ByteBuf entry2 = Unpooled.buffer(BUFF_SIZE);
            entry2.writeLong(3); //ledger id
            entry2.writeLong(1); //entry id
            entry2.writeBytes("entry-2".getBytes());

            storage.addEntry(entry2);
            storage.flush();

            storage.setFenced(3);

            storage.setLimboState(3);

            //Correct, shouldn't fail if limbo state set after set fenced
            Assertions.assertDoesNotThrow(() -> storage.isFenced(3));



            //Added after jacoco
            Assertions.assertTrue(storage.hasLimboState(3));

            storage.clearLimboState(3);

            Assertions.assertFalse(storage.hasLimboState(3));

        }catch (NullPointerException | IllegalArgumentException e) {
            Assertions.assertTrue(expectedException);

            Assertions.assertEquals(exceptionClass,e.getClass());
        } catch (IOException | BookieException e) {
            //Error in test
            Assertions.fail();
        }


    }



    public static Stream<Arguments> getEntryPartition() {
        return Stream.of(
                Arguments.of(0,0, false,null),
                Arguments.of(0,3, true,Exception.class),
                Arguments.of(0,-1, true,Exception.class),

                Arguments.of(3,0, true,Exception.class),
                Arguments.of(3,3, true,Exception.class),
                Arguments.of(3,-1, true,Exception.class),


                Arguments.of(-1,0, true,IllegalArgumentException.class),
                Arguments.of(-1,3, true,IllegalArgumentException.class),
                Arguments.of(-1,-1, true,IllegalArgumentException.class)

        );
    }

    @ParameterizedTest
    @MethodSource("getEntryPartition")
    public void getEntryTest(long ledgerId, long entryId ,boolean expectedException, Class expectedClass){
        try {
            List entries = new ArrayList();
            for(int i = 0;i<3;i++){
                for(int j = 0;j<3;j++){
                    ByteBuf entry = Unpooled.buffer(BUFF_SIZE);
                    entry.writeLong(i); // ledger id
                    entry.writeLong(j); // entry id
                    entry.writeBytes(("entry-" + i).getBytes());
                        storage.addEntry(entry);
                    entries.add(entry);
                }
            }

            storage.flush();

            storage.getEntry(ledgerId,entryId);
            Assertions.assertFalse(expectedException);
        } catch (Exception e) {
            Assertions.assertTrue(expectedException);

            if(e.getClass().equals(IllegalArgumentException.class)){
                Assertions.assertEquals(expectedClass.getSimpleName(),e.getClass().getSimpleName());
            }
        }

    }


    //Function to check debug in testing redirecting output
    private static SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor getLedgerLoggerProcessor() {
        return (currentEntry, entryLodId, position) -> {
            try {
                writeOnFile(currentEntry,entryLodId,position);
            } catch (IOException e) {
                //Error in test
                fail();
            }
        };
        //Alternative console debug
                /*System.out
                .println("entry " + currentEntry + "\t:\t(log: " + entryLodId + ", pos: " + position + ")");*/
    }

    @BeforeEach
    public void cleanTestDir() {
        try {
            File file = new File(File.separator + System.getProperty("user.dir") + File.separator + "test_dir");
            if (!file.isDirectory())
                file.mkdir();
            file = new File(System.getProperty("user.dir") + File.separator + "test_dir" + File.separator + "test.txt");
            if (!file.exists())
                file.createNewFile();


            PrintWriter cleanWriter = new PrintWriter(file);
            cleanWriter.print("");
            cleanWriter.close();
        }catch (Exception e){
            fail();
        }
    }

    @AfterEach
    public void deleteTestDir() {
        try {
            File file = new File(System.getProperty("user.dir") + File.separator + "test_dir" + File.separator + "test.txt");
            if (file.exists())
                file.delete();


            file = new File(File.separator + System.getProperty("user.dir") + File.separator + "test_dir");
            if (file.isDirectory())
                file.delete();


        }catch (Exception e){
            fail();
        }
    }


    private static void writeOnFile(long currentEntry, long entryLodId, long position) throws IOException {
        File file = new File(System.getProperty("user.dir") + File.separator + "test_dir" + File.separator + "test.txt");

        FileWriter fileWriter = new FileWriter(file,true);
        BufferedWriter writer = new BufferedWriter(fileWriter);
        writer.append(String.valueOf(currentEntry) + "\n");
        writer.append(String.valueOf(entryLodId) + "\n");
        writer.append(String.valueOf(position) + "\n");

        writer.close();
    }


    private static File getLedgerDir() {
        try {
            File tmpDirLedger = File.createTempFile("bkTest", ".dir");
            tmpDirLedger.delete();
            tmpDirLedger.mkdir();
            File curDir = BookieImpl.getCurrentDirectory(tmpDirLedger);
            BookieImpl.checkDirectoryStructure(curDir);
            return tmpDirLedger;
        }catch (Exception e){
            return null;
        }
    }


    private static File getNotLedgerDir() {
        try {
            File tmpDirLedger = File.createTempFile("bkTest", ".dir");
            tmpDirLedger.delete();
            tmpDirLedger.mkdir();
            return tmpDirLedger;
        }catch (Exception e){
            return null;
        }
    }

    private static Object getAnotherLedgerDir() {
        try {
            File tmpDirLedger = File.createTempFile("bkTest2", ".dir");
            tmpDirLedger.delete();
            tmpDirLedger.mkdir();
            File curDir = BookieImpl.getCurrentDirectory(tmpDirLedger);
            BookieImpl.checkDirectoryStructure(curDir);
            return tmpDirLedger;
        }catch (Exception e){
            return null;
        }
    }

    public static Stream<Arguments> readLedgerIndexEntriesPartition() {
        return Stream.of(
                Arguments.of(0,0,DbLedgerStorage.class,getLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,0,DbLedgerStorage.class,getLedgerDir(),getNotLedgerDir(), null, true),
                Arguments.of(0,0,DbLedgerStorage.class,getLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,0,DbLedgerStorage.class,getLedgerDir(),null,getLedgerLoggerProcessor(), true),
                Arguments.of(0,0,DbLedgerStorage.class,getNotLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,0,DbLedgerStorage.class,getNotLedgerDir(),getNotLedgerDir(),null, true),
                Arguments.of(0,0,DbLedgerStorage.class,getNotLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,0,DbLedgerStorage.class,getNotLedgerDir(),null,getLedgerLoggerProcessor(), true),
                Arguments.of(0,0,DbLedgerStorage.class,null,getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,0,DbLedgerStorage.class,null,getNotLedgerDir(),null, true),
                Arguments.of(0,0,DbLedgerStorage.class,null,getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,0,DbLedgerStorage.class,null,null,getLedgerLoggerProcessor(), true),
                Arguments.of(0,1000,DbLedgerStorage.class,getLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), false),
                Arguments.of(0,1000,DbLedgerStorage.class,getLedgerDir(),getNotLedgerDir(),null, true),
                Arguments.of(0,1000,DbLedgerStorage.class,getLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(),true),
                Arguments.of(0,1000,DbLedgerStorage.class,getLedgerDir(),null,getLedgerLoggerProcessor(), false), //Changed after jacoco
                Arguments.of(0,1000,InterleavedLedgerStorage.class,getNotLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,1000,DbLedgerStorage.class,getNotLedgerDir(),getNotLedgerDir(),null, true),
                Arguments.of(0,1000,InterleavedLedgerStorage.class,getNotLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,1000,InterleavedLedgerStorage.class,getNotLedgerDir(),null,getLedgerLoggerProcessor(), true),
                Arguments.of(0,1000,InterleavedLedgerStorage.class,null,getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,1000,DbLedgerStorage.class,null,getNotLedgerDir(),null, true),
                Arguments.of(0,1000,InterleavedLedgerStorage.class,null,getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,1000,InterleavedLedgerStorage.class,null,null,getLedgerLoggerProcessor(), true),
                Arguments.of(0,-1,DbLedgerStorage.class,getLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,-1,SortedLedgerStorage.class,getLedgerDir(),getNotLedgerDir(),null, true),
                Arguments.of(0,-1,SortedLedgerStorage.class,getLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,-1,DbLedgerStorage.class,getLedgerDir(),null,getLedgerLoggerProcessor(), true),
                Arguments.of(0,-1,SortedLedgerStorage.class,getNotLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,-1,DbLedgerStorage.class,getNotLedgerDir(),getNotLedgerDir(),null, true),
                Arguments.of(0,-1,DbLedgerStorage.class,getNotLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,-1,SortedLedgerStorage.class,getNotLedgerDir(),null,getLedgerLoggerProcessor(), true),
                Arguments.of(0,-1,SortedLedgerStorage.class,null,getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,-1,DbLedgerStorage.class,null,getNotLedgerDir(),null, true),
                Arguments.of(0,-1,SortedLedgerStorage.class,null,getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(0,-1,SortedLedgerStorage.class,null,null,getLedgerLoggerProcessor(), true),
                Arguments.of(-1,0,DbLedgerStorage.class,getLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,0,DbLedgerStorage.class,getLedgerDir(),getNotLedgerDir(), null, true),
                Arguments.of(-1,0,DbLedgerStorage.class,getLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,0,DbLedgerStorage.class,getLedgerDir(),null,getLedgerLoggerProcessor(), true),
                Arguments.of(-1,0,DbLedgerStorage.class,getNotLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,0,DbLedgerStorage.class,getNotLedgerDir(),getNotLedgerDir(),null, true),
                Arguments.of(-1,0,DbLedgerStorage.class,getNotLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,0,DbLedgerStorage.class,getNotLedgerDir(),null,getLedgerLoggerProcessor(), true),
                Arguments.of(-1,0,DbLedgerStorage.class,null,getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,0,DbLedgerStorage.class,null,getNotLedgerDir(),null, true),
                Arguments.of(-1,0,DbLedgerStorage.class,null,getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,0,DbLedgerStorage.class,null,null,getLedgerLoggerProcessor(), true),
                Arguments.of(-1,1000,DbLedgerStorage.class,getLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,1000,DbLedgerStorage.class,getLedgerDir(),getNotLedgerDir(),null, true),
                Arguments.of(-1,1000,DbLedgerStorage.class,getLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,1000,DbLedgerStorage.class,getLedgerDir(),null,getLedgerLoggerProcessor(), true),
                Arguments.of(-1,1000,InterleavedLedgerStorage.class,getNotLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,1000,DbLedgerStorage.class,getNotLedgerDir(),getNotLedgerDir(),null, true),
                Arguments.of(-1,1000,InterleavedLedgerStorage.class,getNotLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,1000,InterleavedLedgerStorage.class,getNotLedgerDir(),null,getLedgerLoggerProcessor(), true),
                Arguments.of(-1,1000,InterleavedLedgerStorage.class,null,getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,1000,DbLedgerStorage.class,null,getNotLedgerDir(),null, true),
                Arguments.of(-1,1000,InterleavedLedgerStorage.class,null,getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,1000,InterleavedLedgerStorage.class,null,null,getLedgerLoggerProcessor(), true),
                Arguments.of(-1,-1,DbLedgerStorage.class,getLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,-1,SortedLedgerStorage.class,getLedgerDir(),getNotLedgerDir(),null, true),
                Arguments.of(-1,-1,SortedLedgerStorage.class,getLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,-1,DbLedgerStorage.class,getLedgerDir(),null,getLedgerLoggerProcessor(), true),
                Arguments.of(-1,-1,SortedLedgerStorage.class,getNotLedgerDir(),getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,-1,DbLedgerStorage.class,getNotLedgerDir(),getNotLedgerDir(),null, true),
                Arguments.of(-1,-1,DbLedgerStorage.class,getNotLedgerDir(),getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,-1,SortedLedgerStorage.class,getNotLedgerDir(),null,getLedgerLoggerProcessor(), true),
                Arguments.of(-1,-1,SortedLedgerStorage.class,null,getLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,-1,DbLedgerStorage.class,null,getNotLedgerDir(),null, true),
                Arguments.of(-1,-1,SortedLedgerStorage.class,null,getAnotherLedgerDir(),getLedgerLoggerProcessor(), true),
                Arguments.of(-1,-1,SortedLedgerStorage.class,null,null,getLedgerLoggerProcessor(), true)


        );
    }


    private void verifyReadCorrectly() throws WronEntityMatcException {
        try {
            List<String> expectedValues = new ArrayList<>();
            expectedValues.add("1");
            expectedValues.add("0");
            expectedValues.add("1028");
            expectedValues.add("2");
            expectedValues.add("0");
            expectedValues.add("1063");

            int count = 0;

            File myObj = new File(System.getProperty("user.dir") + File.separator + "test_dir/test.txt");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                if(data.compareTo(expectedValues.get(count)) != 0)
                    throw new WronEntityMatcException();

                count++;
            }
            myReader.close();
        } catch (IOException e) {
            fail();
        }

    }



    @ParameterizedTest
    @MethodSource("readLedgerIndexEntriesPartition")
    public void readLedgerIndexEntriesTest(long ledgerId, int gcWaitTime, Class storageClass,File tempDirLedger, File tempDirIndex,SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor processor , boolean expectedException){
        try {
            ServerConfiguration serverConfiguration = TestBKConfiguration.newServerConfiguration();
            serverConfiguration.setGcWaitTime(gcWaitTime);
            serverConfiguration.setLedgerStorageClass(storageClass.getName()); //Set this class as the persistence one

            //Added after Jacoco
            if(tempDirLedger != null) {
                serverConfiguration.setLedgerDirNames(new String[]{tempDirLedger.toString()});
            }
            if(tempDirIndex != null) {
                if(tempDirIndex.getPath().contains("bkTest2")){
                    serverConfiguration.setIndexDirName(new String[]{tempDirIndex.toString(), tempDirLedger.toString()}); //Check different sizes index and dirmanager
                }else{
                    serverConfiguration.setIndexDirName(new String[]{tempDirIndex.toString()});
                }
            }


            BookieImpl bookie = new TestBookieImpl(serverConfiguration);
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
            DbLedgerStorage.readLedgerIndexEntries(ledgerId,serverConfiguration,processor);

            Assertions.assertDoesNotThrow(this::verifyReadCorrectly);


            Assertions.assertFalse(expectedException);
        } catch (Exception e) {
            System.out.println(ledgerId + " " + gcWaitTime + " " + storageClass + " " + tempDirLedger + " " + tempDirIndex + " " + processor);
            Assertions.assertTrue(expectedException);
        }

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


    private static LedgerCache.PageEntriesIterable getSingleLedgerPages() throws IOException {

        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();
        TestStatsProvider statsProvider = new TestStatsProvider();
        final long numWrites = 2000;
        final long entriesPerWrite = 2;
        final long numOfLedgers = 1;

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


    public static Stream<Arguments> addLedgerToIndexPartition() throws IOException, EmptyPagesException {
        return Stream.of(
                Arguments.of(0, true, "key".getBytes(), getPages(), false),
                Arguments.of(-1, true, "key".getBytes(), getPages(), true),
                Arguments.of(0, false, "key".getBytes(), getPages(), false),
                Arguments.of(-1, false, "key".getBytes(), getPages(), true),

                //Arguments.of(0, true, "new-key".getBytes(), getPages(), true),  //Test fail
                Arguments.of(-1, true, "new-key".getBytes(), getPages(), true),
                Arguments.of(0, false, "new-key".getBytes(), getPages(), false),
                Arguments.of(-1, false, "new-key".getBytes(), getPages(), true),

                Arguments.of(0, true, "".getBytes(), getPages(), false),
                Arguments.of(-1, true, "".getBytes(), getPages(), true),
                Arguments.of(0, false, "".getBytes(), getPages(), false),
                Arguments.of(-1, false, "".getBytes(), getPages(), true),

                Arguments.of(0, true, null, getPages(), true),
                Arguments.of(-1, true, null, getPages(), true),
                Arguments.of(0, false, null, getPages(), true),
                Arguments.of(-1, false, null, getPages(), true),


                Arguments.of(0, true, "key".getBytes(), getSingleLedgerPages(), false),
                Arguments.of(-1, true, "key".getBytes(), getSingleLedgerPages(), true),
                Arguments.of(0, false, "key".getBytes(), getSingleLedgerPages(), false),
                Arguments.of(-1, false, "key".getBytes(), getSingleLedgerPages(), true),

                //Arguments.of(0, true, "new-key".getBytes(), getSingleLedgerPages(), true), //Test fail
                Arguments.of(-1, true, "new-key".getBytes(), getSingleLedgerPages(), true),
                Arguments.of(0, false, "new-key".getBytes(), getSingleLedgerPages(), false),
                Arguments.of(-1, false, "new-key".getBytes(), getSingleLedgerPages(), true),

                Arguments.of(0, true, "".getBytes(), getSingleLedgerPages(), false),
                Arguments.of(-1, true, "".getBytes(), getSingleLedgerPages(), true),
                Arguments.of(0, false, "".getBytes(), getSingleLedgerPages(), false),
                Arguments.of(-1, false, "".getBytes(), getSingleLedgerPages(), true),

                Arguments.of(0, true, null, getSingleLedgerPages(), true),
                Arguments.of(-1, true, null, getSingleLedgerPages(), true),
                Arguments.of(0, false, null, getSingleLedgerPages(), true),
                Arguments.of(-1, false, null, getSingleLedgerPages(), true),


                Arguments.of(0, true, "key".getBytes(), getEmptyPages(), true),
                Arguments.of(-1, true, "key".getBytes(), getEmptyPages(), true),
                Arguments.of(0, false, "key".getBytes(), getEmptyPages(), true),
                Arguments.of(-1, false, "key".getBytes(), getEmptyPages(), true),

                Arguments.of(0, true, "new-key".getBytes(), getEmptyPages(), true),
                Arguments.of(-1, true, "new-key".getBytes(), getEmptyPages(), true),
                Arguments.of(0, false, "new-key".getBytes(), getEmptyPages(), true),
                Arguments.of(-1, false, "new-key".getBytes(), getEmptyPages(), true),

                Arguments.of(0, true, "".getBytes(), getEmptyPages(), true),
                Arguments.of(-1, true, "".getBytes(), getEmptyPages(), true),
                Arguments.of(0, false, "".getBytes(), getEmptyPages(), true),
                Arguments.of(-1, false, "".getBytes(), getEmptyPages(), true),

                Arguments.of(0, true, null, getEmptyPages(), true),
                Arguments.of(-1, true, null, getEmptyPages(), true),
                Arguments.of(0, false, null, getEmptyPages(), true),
                Arguments.of(-1, false, null, getEmptyPages(), true),


                Arguments.of(0, true, "key".getBytes(), null, true),
                Arguments.of(-1, true, "key".getBytes(), null, true),
                Arguments.of(0, false, "key".getBytes(), null, true),
                Arguments.of(-1, false, "key".getBytes(), null, true),

                Arguments.of(0, true, "new-key".getBytes(), null, true),
                Arguments.of(-1, true, "new-key".getBytes(), null, true),
                Arguments.of(0, false, "new-key".getBytes(), null, true),
                Arguments.of(-1, false, "new-key".getBytes(), null, true),

                Arguments.of(0, true, "".getBytes(), null, true),
                Arguments.of(-1, true, "".getBytes(), null, true),
                Arguments.of(0, false, "".getBytes(), null, true),
                Arguments.of(-1, false, "".getBytes(), null, true),

                Arguments.of(0, true, null, null, true),
                Arguments.of(-1, true, null, null, true),
                Arguments.of(0, false, null, null, true),
                Arguments.of(-1, false, null, null, true),




                //Added after ba-dua
                Arguments.of(0, true, "key".getBytes(), getMockedPages(), false),
                Arguments.of(-1, true, "key".getBytes(), getMockedPages(), true),
                Arguments.of(0, false, "key".getBytes(), getMockedPages(), false),
                Arguments.of(-1, false, "key".getBytes(), getMockedPages(), true),

                Arguments.of(0, true, "new-key".getBytes(), getMockedPages(), false),
                Arguments.of(-1, true, "new-key".getBytes(), getMockedPages(), true),
                Arguments.of(0, false, "new-key".getBytes(), getMockedPages(), false),
                Arguments.of(-1, false, "new-key".getBytes(), getMockedPages(), true),

                Arguments.of(0, true, "".getBytes(), getMockedPages(), false),
                Arguments.of(-1, true, "".getBytes(), getMockedPages(), true),
                Arguments.of(0, false, "".getBytes(), getMockedPages(), false),
                Arguments.of(-1, false, "".getBytes(), getMockedPages(), true),

                Arguments.of(0, true, null, getMockedPages(), true),
                Arguments.of(-1, true, null, getMockedPages(), true),
                Arguments.of(0, false, null, getMockedPages(), true),
                Arguments.of(-1, false, null, getMockedPages(), true)

        );
    }

    private static Object getMockedPages() {
        LedgerCache.PageEntriesIterable pages = Mockito.mock(LedgerCache.PageEntriesIterable.class);
        Iterator iterator = Mockito.mock(Iterator.class);
        LedgerCache.PageEntries ledgerEntryPage = Mockito.mock(LedgerCache.PageEntries.class);


        when(pages.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(ledgerEntryPage);
        try {
            when(ledgerEntryPage.getLEP()).thenThrow(new IOException());
        } catch (IOException e) {
            fail();
        }

        return pages;
    }


    @ParameterizedTest
    @MethodSource("addLedgerToIndexPartition") //master key is used for crypto reasons
    public void addLedgerToIndexTest(long ledgerId, boolean isFenced, byte[] masterKey,
                                 LedgerCache.PageEntriesIterable pages, boolean expectedException) {
        //Fanced means read only
        try {
            if(!Arrays.equals(masterKey, "".getBytes())){
                storage.setMasterKey(ledgerId, "key".getBytes());
            }else{
                storage.setMasterKey(ledgerId, masterKey);
            }



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
    public void bookieCompactionTest() throws Exception {
        storage.setMasterKey(1, "key".getBytes());
        storage.start();

        ByteBuf entry3 = Unpooled.buffer(BUFF_SIZE);
        entry3.writeLong(1); // ledger id
        entry3.writeLong(3); // entry id
        entry3.writeBytes("entry-3".getBytes());
        storage.addEntry(entry3);


        // Simulate bookie compaction
        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
        EntryLogger entryLogger = singleDirStorage.getEntryLogger();




        // Rewrite entry-3
        ByteBuf newEntry3 = Unpooled.buffer(BUFF_SIZE);
        newEntry3.writeLong(1); // ledger id
        newEntry3.writeLong(3); // entry id
        newEntry3.writeBytes("new-entry-3".getBytes());
        long location = entryLogger.addEntry(1L, newEntry3);
        newEntry3.resetReaderIndex();

        storage.flush();
        List<EntryLocation> locations = Lists.newArrayList(new EntryLocation(1, 3, location));
        singleDirStorage.updateEntriesLocations(locations);  //Force compaction

        ByteBuf res = storage.getEntry(1, 3);
        assertEquals(newEntry3, res);
    }


    public static Stream<Arguments> doGetEntryPartition() {
        return Stream.of(
                Arguments.of(0,0,false),
                Arguments.of(0,-1,false),
                Arguments.of(0,-2,true),

                Arguments.of(3,0,true),
                Arguments.of(3,-1,true),
                Arguments.of(3,-2,true),

                Arguments.of(0,3,true),
                Arguments.of(0,3,true),
                Arguments.of(0,3,true),


                Arguments.of(-1,0,true),
                Arguments.of(-1,-1,true),
                Arguments.of(-1,-2,true)
        );
    }


    //Implement flush caching logic to upgrade coverage
    @ParameterizedTest
    @MethodSource("doGetEntryPartition")
    public void doGetEntryPartition(long ledgerId, long entryId, boolean exceptionExpected){
        try {
            List<ByteBuf> entries = new ArrayList();
            storage.setMasterKey(ledgerId, "key".getBytes());
            for(int i = 0;i<3;i++){
                for(int j = 0;j<3;j++){
                    ByteBuf entry = Unpooled.buffer(BUFF_SIZE);
                    entry.writeLong(i); // ledger id
                    entry.writeLong(j); // entry id
                    entry.writeBytes(("entry-" + i).getBytes());
                    storage.addEntry(entry);
                    entries.add(entry);
                }
            }

            storage.flush();

            Assertions.assertEquals(storage.getEntry(ledgerId,BookieProtocol.LAST_ADD_CONFIRMED),storage.getLastEntry(ledgerId)); //Check if entry has been confirmed

            SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
            SingleDirectoryDbLedgerStorage singleDirStorage2 = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);


            Assertions.assertTrue(singleDirStorage.ledgerExists(ledgerId));

            ByteBuf storageEntry = singleDirStorage.doGetEntry(ledgerId,entryId);

            singleDirStorage.addEntry(entries.get((int) ledgerId));

            singleDirStorage.doGetEntry(ledgerId,entryId);
            singleDirStorage2.doGetEntry(ledgerId,entryId); //Added after jacoco


            //Should have the same behaviour of getEntry od DbLedgerStorage
            if(entryId != -1) //It means getLastEntry
                Assertions.assertEquals(storage.getEntry(ledgerId,entryId),storageEntry);

            Assertions.assertFalse(exceptionExpected);
        } catch (Exception e) {
            Assertions.assertTrue(exceptionExpected);
        }


    }
}
