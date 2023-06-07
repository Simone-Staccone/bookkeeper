package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doNothing;


public class LedgerDescriptorImplTest{
    private LedgerStorage ledgerStorage;

    public LedgerDescriptorImplTest(){
        setUp();
    }

    public void setUp() {
        try {
            File tmpDir = File.createTempFile("bkTest", ".dir");
            tmpDir.delete();
            tmpDir.mkdir();
            File curDir = BookieImpl.getCurrentDirectory(tmpDir);
            BookieImpl.checkDirectoryStructure(curDir);

            ServerConfiguration serverConfiguration = TestBKConfiguration.newServerConfiguration();
            serverConfiguration.setLedgerStorageClass(DbLedgerStorage.class.getName());
            serverConfiguration.setLedgerDirNames(new String[]{tmpDir.toString()});
            BookieImpl bookie = new TestBookieImpl(serverConfiguration);
            this.ledgerStorage = (DbLedgerStorage) bookie.getLedgerStorage();


        } catch (Exception e) {
            System.err.println("Setup failed");
        }
    }


    private static LedgerStorage getLedgerStorageConfiguration(int type, long ledgerId) throws Exception {
        LedgerStorage ledgerStorage = null;
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        ServerConfiguration serverConfiguration = TestBKConfiguration.newServerConfiguration();
        serverConfiguration.setLedgerStorageClass(DbLedgerStorage.class.getName());
        serverConfiguration.setLedgerDirNames(new String[]{tmpDir.toString()});
        BookieImpl bookie = new TestBookieImpl(serverConfiguration);

        switch (type) {
            case 1: //Ledger Storage with entries
                ledgerStorage = bookie.getLedgerStorage();

                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(1); //Entry id
                entry.writeBytes("entry-example-1".getBytes());


                ledgerStorage.addEntry(entry);

                ledgerStorage.flush();


                break;
            case 2: //Empty ledger storage
                ledgerStorage = bookie.getLedgerStorage();
                break;
            case 3: //Not initialized ledger storage
                ledgerStorage = new DbLedgerStorage();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        return ledgerStorage;
    }



    public static Stream<Arguments> createPartition() {
        return Stream.of(
                Arguments.of("masterKey".getBytes(), 1, 1,false),
                Arguments.of("masterKey".getBytes(), 1, 2,false),
                Arguments.of("masterKey".getBytes(), 1, 3,true),

                Arguments.of("".getBytes(), 1, 1,false),
                Arguments.of("".getBytes(), 1, 2, false),
                Arguments.of("".getBytes(), 1, 3, true),

                Arguments.of(null, 1, 1, true),
                Arguments.of(null, 1, 2, true),
                Arguments.of(null, 1, 3, true),



                Arguments.of("masterKey".getBytes(), 0, 1,false),
                Arguments.of("masterKey".getBytes(), 0, 2,false),
                Arguments.of("masterKey".getBytes(), 0, 3,true),

                Arguments.of("".getBytes(), 0, 1,false),
                Arguments.of("".getBytes(), 0, 2, false),
                Arguments.of("".getBytes(), 0, 3, true),

                Arguments.of(null, 0, 1, true),
                Arguments.of(null, 0, 2, true),
                Arguments.of(null, 0, 3, true),



                Arguments.of("masterKey".getBytes(), -1, 1,true),
                Arguments.of("masterKey".getBytes(), -1, 2,false),  //If I don't add any entry I don't want to throw errors
                Arguments.of("masterKey".getBytes(), -1, 3,true),

                Arguments.of("".getBytes(), -1, 1,true),
                Arguments.of("".getBytes(), -1, 2, false),
                Arguments.of("".getBytes(), -1, 3, true),

                Arguments.of(null, -1, 1, true),
                Arguments.of(null, -1, 2, true),
                Arguments.of(null, -1, 3, true)

                );
    }



    @ParameterizedTest
    @MethodSource("createPartition")
    public void testCreate(byte[] masterKey, long ledgerId,int ledgerConfiguration, boolean expectedException) {
        try {
            LedgerStorage ledgerStorage = getLedgerStorageConfiguration(ledgerConfiguration,ledgerId);

            ledgerStorage.setMasterKey(ledgerId,masterKey); //Set master key and set metadata

            Assertions.assertNotEquals("anotherMasterKey".getBytes(),ledgerStorage.readMasterKey(ledgerId));

            LedgerDescriptorImpl ledgerDescriptor = new LedgerDescriptorImpl(masterKey,ledgerId,ledgerStorage);


            Assertions.assertNotNull(ledgerDescriptor);

            Assertions.assertDoesNotThrow(() -> ledgerDescriptor.checkAccess(masterKey));

            ledgerDescriptor.isFenced(); //Check fenced to trigger exceptions on ledger descriptor
            Assertions.assertFalse(expectedException);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.assertTrue(expectedException);
        }


    }

    public static Stream<Arguments> fencedPartition() {
        return Stream.of(
                Arguments.of(true),
                Arguments.of(false)
        );
    }

    @ParameterizedTest
    @MethodSource("fencedPartition")
    public void testFenced(boolean setFenced) throws IOException, BookieException {
        this.ledgerStorage.setMasterKey(1,"key".getBytes());
        LedgerDescriptorImpl ledgerDescriptor = new LedgerDescriptorImpl("key".getBytes(),1,this.ledgerStorage);
        ByteBuf entry = Unpooled.buffer(128);
        entry.writeLong(1);
        entry.writeLong(1);
        entry.writeBytes("entry".getBytes());

        ledgerDescriptor.addEntry(entry);
        Assertions.assertEquals(entry,ledgerDescriptor.readEntry(1));


        if(setFenced){
            ledgerDescriptor.setFenced();
            Assertions.assertTrue(this.ledgerStorage.isFenced(1));
        }

        Assertions.assertEquals(setFenced,ledgerDescriptor.isFenced());
    }


    public static Stream<Arguments> testFenceAndLogInJournalPartition() throws InterruptedException {
        return Stream.of(
                Arguments.of(getJournalConfiguration(1),false),
                Arguments.of(getJournalConfiguration(2),false),
                Arguments.of(null,true)

        );
    }


    private static Journal getJournalConfiguration(int type) throws InterruptedException {
        List<ByteBuf> mockState = new ArrayList<>();
        Journal journalSpy = Mockito.spy(Mockito.mock(Journal.class));
        //Mockito.doAnswer(i -> mockState.add(i.getArgument(0)))
        doNothing().when(journalSpy).logAddEntry(any(),anyBoolean(),any(),any());
        switch (type) {
            case 1:
                //Populate journal
                for(int i =0;i<10;i++){
                    ByteBuf entry = Unpooled.buffer(128);
                    entry.writeLong(i);
                    entry.writeLong(i);
                    entry.writeBytes(("entry" + i).getBytes());
                    mockState.add(entry);
                }
                break;
            case 2:
                //Empty journal
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }


        return journalSpy;
    }

    @ParameterizedTest
    @MethodSource("testFenceAndLogInJournalPartition")
    public void testFenceAndLogInJournal(Journal journal,boolean expectedException) {
        try {

            this.ledgerStorage.setMasterKey(1,"key".getBytes());
            LedgerDescriptorImpl ledgerDescriptor = new LedgerDescriptorImpl("key".getBytes(),1,this.ledgerStorage);

            ledgerDescriptor.setFenced();
            Assertions.assertTrue(ledgerDescriptor.fenceAndLogInJournal(journal).isDone());


            this.ledgerStorage.setMasterKey(2,"key".getBytes());
            LedgerDescriptorImpl ledgerDescriptor2 = new LedgerDescriptorImpl("key".getBytes(),2,this.ledgerStorage);

            Assertions.assertFalse(ledgerDescriptor2.fenceAndLogInJournal(journal).isDone());

            Assertions.assertFalse(expectedException);
        } catch (NullPointerException | IOException e) {
            Assertions.assertTrue(expectedException);
        }

    }

}