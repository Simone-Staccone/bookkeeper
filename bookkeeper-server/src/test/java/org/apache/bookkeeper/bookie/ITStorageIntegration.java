package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.SingleDirectoryDbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.*;

public class ITStorageIntegration {
    private DbLedgerStorage dbLedgerStorage;
    private SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage;
    private LedgerDescriptorImplTest ledgerDescriptorImplTest;
    private final List<ByteBuf> entryState = new ArrayList<>();
    private final int BUFF_SIZE = 256;


    @BeforeEach
    public void setUp() throws IOException {
        ServerConfiguration serverConfiguration = mock(ServerConfiguration.class);
        LedgerManager ledgerManager = mock(LedgerManager.class);
        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        LedgerDirsManager indexDirsManager = mock(LedgerDirsManager.class);
        EntryLogger entryLogger = mock(EntryLogger.class);
        StatsLogger statsLogger = mock(StatsLogger.class);
        ByteBufAllocator byteBufAllocator = mock(ByteBufAllocator.class);
        BookieImpl bookie = mock(BookieImpl.class);
        OpStatsLogger opStatsLogger = mock(OpStatsLogger.class);

        File tmpDir = File.createTempFile("bkTestDir", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        File indexDir = File.createTempFile("bkTestIndex", ".dir");
        indexDir.delete();
        indexDir.mkdir();
        File curDirIndex = BookieImpl.getCurrentDirectory(indexDir);
        BookieImpl.checkDirectoryStructure(curDirIndex);



        when(byteBufAllocator.buffer(30 ,30)).thenReturn(Unpooled.buffer()); //this is the size of the entry that will be added after in the test

        //when(opStatsLogger.registerSuccessfulEvent()).thenReturn();

        when(statsLogger.getThreadScopedOpStatsLogger("add-entry")).thenReturn(opStatsLogger);
        when(statsLogger.getThreadScopedOpStatsLogger("read-entry")).thenReturn(opStatsLogger);

        when(statsLogger.getCounter("write-cache-hits")).thenReturn(mock(Counter.class));
        when(statsLogger.getCounter("write-cache-misses")).thenReturn(mock(Counter.class));
        when(statsLogger.getCounter("read-cache-hits")).thenReturn(mock(Counter.class));
        when(statsLogger.getCounter("read-cache-misses")).thenReturn(mock(Counter.class));




        when(ledgerDirsManager.getAllLedgerDirs()).thenReturn(Arrays.asList(curDir)); //Set single directory to see if right storage is started
        when(indexDirsManager.getAllLedgerDirs()).thenReturn(Arrays.asList(curDirIndex)); //Set single directory to see if right storage is started


        when(entryLogger.getFlushedLogIds()).thenAnswer(i -> returnLongCollection());
        when(statsLogger.scopeLabel("ledgerDir",curDir.toString())).thenReturn(statsLogger);
        when(statsLogger.scopeLabel("indexDir",curDirIndex.toString())).thenReturn(statsLogger);


        when(serverConfiguration.getGcWaitTime()).thenReturn(1000L); //Default value according to documentation

        when(serverConfiguration.getLedgerDirs()).thenReturn(tmpDir.listFiles());
        when(serverConfiguration.getIndexDirs()).thenReturn(indexDir.listFiles());


        when(serverConfiguration.getLedgerDirNames()).thenReturn(new String[] {tmpDir.toString()} );
        when(serverConfiguration.getIndexDirNames()).thenReturn(new String[] {indexDir.toString()} );
        when(serverConfiguration.getLedgerMetadataRocksdbConf()).thenReturn(File.separator + "conf" + File.separator + "ledger_metadata_rocksdb.conf");  //See documentation to get this string
        when(serverConfiguration.getEntryLocationRocksdbConf()).thenReturn(File.separator + "conf" + File.separator + "entry_location_rocksdb.conf");  //See documentation to get this string
        when(serverConfiguration.getDefaultRocksDBConf()).thenReturn(File.separator + "conf" + File.separator + "default_rocksdb.conf");  //See documentation to get this string


        when(serverConfiguration.getString("dbStorage_rocksDB_checksum_type", "kxxHash")).thenReturn("kxxHash");
        when(serverConfiguration.getString("dbStorage_rocksDB_logPath", "")).thenReturn("");
        when(serverConfiguration.getString("dbStorage_rocksDB_logLevel","info")).thenReturn("info");


        when(serverConfiguration.getCompactionRate()).thenReturn(1000); //Default compaction rate according to documentation
        when(serverConfiguration.getCompactionRateByBytes()).thenReturn(1000000); //Default compaction rate according to documentation
        when(serverConfiguration.getCompactionRateByEntries()).thenReturn(1000); //Default compaction rate according to documentation


//        //RockDB creation via static mock
//        try (MockedStatic<RocksDB> rocksDBMockedStatic = Mockito.mockStatic(RocksDB.class)) {
//            rocksDBMockedStatic.when(() -> RocksDB.open(any(),any())).thenReturn(Mockito.mock(RocksDB.class));
//            Assertions.assertEquals(RocksDB.class,RocksDB.open(any(),any()).getClass());
//        }

//        try (MockedStatic<RocksDB> mocked = mockStatic(RocksDB.class)) {
//            mocked.when(() -> RocksDB.open(new Options(),"")).thenReturn(Mockito.mock(RocksDB.class));
//        }
//
//
//        SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage
//                = new SingleDirectoryDbLedgerStorage(
//                serverConfiguration,
//                ledgerManager,
//                ledgerDirsManager,
//                indexDirsManager,
//                entryLogger,
//                statsLogger,
//                byteBufAllocator,
//                1024,
//                1024,
//                64
//        );

        when(bookie.getLedgerStorage()).thenReturn(singleDirectoryDbLedgerStorage);

        this.dbLedgerStorage = new DbLedgerStorage();

        this.dbLedgerStorage.initialize(serverConfiguration,
                ledgerManager,
                ledgerDirsManager,
                indexDirsManager,
                statsLogger,
                byteBufAllocator);



        //this.singleDirectoryDbLedgerStorage =

    }

    private Collection<Long> returnLongCollection() {
        Collection<Long> longCollection = new ArrayList<>();
        longCollection.add(1L);
        longCollection.add(2L);
        longCollection.add(3L);
        return  longCollection;
    }


    //Test to mock if ledger actually modifies the right storage in persistence
    @Test
    public void ledgerDescriptorStorageTest(){
        ByteBuf entry = Unpooled.buffer(BUFF_SIZE);
        entry.writeLong(1); //Ledger id
        entry.writeLong(1); //Entry id
        entry.writeBytes("entry".getBytes());

        try {
            this.dbLedgerStorage.start();
            this.dbLedgerStorage.addEntry(entry);
            this.dbLedgerStorage.setMasterKey(1,"masterKey".getBytes());



            DbLedgerStorage storageSpy = Mockito.spy(this.dbLedgerStorage);

            LedgerDescriptorImpl ledgerDescriptor = new LedgerDescriptorImpl("masterKey".getBytes(),1,storageSpy);

            LedgerDescriptorImpl ledgerDescriptor2 = new LedgerDescriptorImpl("masterKey".getBytes(),1,storageSpy);



            ByteBuf entry2 = Unpooled.buffer(BUFF_SIZE);
            entry2.writeLong(1); //Ledger id
            entry2.writeLong(2); //Entry id
            entry2.writeLong(1); //last acked entry
            entry2.writeBytes("entry2".getBytes());

            ledgerDescriptor.addEntry(entry2);

            verify(storageSpy,times(1)).addEntry(entry2); //Check if ledgerDescriptor uses storage to call addEntry

            ledgerDescriptor.setFenced();

            Assertions.assertTrue(this.dbLedgerStorage.isFenced(1));

            verify(storageSpy,times(1)).setFenced(1); //Check if ledgerDescriptor uses storage to call isFenced


            ledgerDescriptor.getLastAddConfirmed();

            verify(storageSpy,times(1)).getLastAddConfirmed(1); //lac set, if verify ok, then message arrived


            Assertions.assertEquals(ledgerDescriptor.getLastAddConfirmed(),storageSpy.getLastAddConfirmed(1));

            Assertions.assertEquals(ledgerDescriptor.getLastAddConfirmed(),ledgerDescriptor2.getLastAddConfirmed());


            ByteBuf entry3 = Unpooled.buffer(BUFF_SIZE);
            entry2.writeLong(1); //Ledger id
            entry2.writeLong(3); //Entry id
            entry2.writeLong(2); //last acked entry
            entry2.writeBytes("entry3".getBytes());


            storageSpy.addEntry(entry3);

            //After add entry when fenced dbLedgerStorage should not have last entry
            Assertions.assertThrows(Exception.class,() -> this.dbLedgerStorage.getLastEntry(1)); //Check if instance of dbLedgerStorage has set fence = true for ledger 1


            Assertions.assertTrue(ledgerDescriptor2.isFenced());

        } catch (IOException | BookieException e) {
            Assertions.assertDoesNotThrow(this::ledgerDescriptorStorageTest);
        }
    }
}
