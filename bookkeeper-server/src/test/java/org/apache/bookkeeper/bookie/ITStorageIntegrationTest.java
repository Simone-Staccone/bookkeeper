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
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class ITStorageIntegrationTest {
    private DbLedgerStorage dbLedgerStorage;
    private SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage;
    private LedgerDescriptorImplTest ledgerDescriptorImplTest;
    private List<ByteBuf> entryState = new ArrayList<>();
    private final int BUFF_SIZE = 256;


    @BeforeEach
    public void setUp() throws IOException {
        ServerConfiguration serverConfiguration = Mockito.mock(ServerConfiguration.class);
        LedgerManager ledgerManager = Mockito.mock(LedgerManager.class);
        LedgerDirsManager ledgerDirsManager = Mockito.mock(LedgerDirsManager.class);
        LedgerDirsManager indexDirsManager = Mockito.mock(LedgerDirsManager.class);
        EntryLogger entryLogger = Mockito.mock(EntryLogger.class);
        StatsLogger statsLogger = Mockito.mock(StatsLogger.class);
        ByteBufAllocator byteBufAllocator = Mockito.mock(ByteBufAllocator.class);
        BookieImpl bookie = Mockito.mock(BookieImpl.class);
        OpStatsLogger opStatsLogger = Mockito.mock(OpStatsLogger.class);


        String metaDataString = File.separator + "path" + File.separator + "to" + File.separator + "ledger" + File.separator + "current";
        String indexString = File.separator + "path" + File.separator + "to" + File.separator + "ledger" + File.separator + "current";
        File metaDataFile = new File(metaDataString + File.separator);
        File indexFile = new File(indexString + File.separator);
        String[] metaDataStrings = {metaDataString};
        String[] indexStrings = {indexString};
        metaDataFile.mkdir();
        indexFile.mkdir();


        when(byteBufAllocator.buffer(30 ,30)).thenReturn(Unpooled.buffer()); //this is the size of the entry that will be added after in the test

        //when(opStatsLogger.registerSuccessfulEvent()).thenReturn();

        when(statsLogger.getThreadScopedOpStatsLogger("add-entry")).thenReturn(opStatsLogger);
        when(statsLogger.getThreadScopedOpStatsLogger("read-entry")).thenReturn(opStatsLogger);

        when(statsLogger.getCounter("write-cache-hits")).thenReturn(Mockito.mock(Counter.class));
        when(statsLogger.getCounter("write-cache-misses")).thenReturn(Mockito.mock(Counter.class));
        when(statsLogger.getCounter("read-cache-hits")).thenReturn(Mockito.mock(Counter.class));
        when(statsLogger.getCounter("read-cache-misses")).thenReturn(Mockito.mock(Counter.class));




        when(ledgerDirsManager.getAllLedgerDirs()).thenReturn(Collections.singletonList(metaDataFile)); //Set single directory to see if right storage is started
        when(indexDirsManager.getAllLedgerDirs()).thenReturn(Collections.singletonList(indexFile)); //Set single directory to see if right storage is started


        when(entryLogger.getFlushedLogIds()).thenAnswer(i -> returnLongCollection());
        when(statsLogger.scopeLabel("ledgerDir",metaDataString)).thenReturn(statsLogger);
        when(statsLogger.scopeLabel("indexDir",indexString)).thenReturn(statsLogger);


        when(serverConfiguration.getGcWaitTime()).thenReturn(1000L); //Default value according to documentation

        when(serverConfiguration.getLedgerDirs()).thenReturn(metaDataFile.listFiles());
        when(serverConfiguration.getIndexDirs()).thenReturn(indexFile.listFiles());


        when(serverConfiguration.getLedgerDirNames()).thenReturn(metaDataStrings);
        when(serverConfiguration.getIndexDirNames()).thenReturn(indexStrings);
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


        } catch (IOException | BookieException e) {
            Assertions.assertDoesNotThrow(this::ledgerDescriptorStorageTest);
        }
    }
}
