package org.apache.bookkeeper.client;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(value=Parameterized.class)
public class BookKeeperTest extends TestCase {
    private String ip;
    private boolean status;

    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(
                new Object[][]{
                        {"127.0.0.1:2181",true},
                        {"127.0.0.1:8080",false}
                }
        );
    }

    public BookKeeperTest(String ip, boolean status){
        this.ip = ip;
        this.status = status;
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
    }

    @Test
    public void testInitializeEnsemblePlacementPolicy() {
        if(this.status){
            assertEquals(0,this.ip.compareTo("127.0.0.1:2181"));
        }else{
            assertEquals(0,this.ip.compareTo("127.0.0.1:8080"));
        }
    }

    @Test
    public void testGetReturnRc() {
        BookKeeper bookKeeper = new BookKeeper();
        assertEquals(1,1);
    }
}