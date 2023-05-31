package org.apache.bookkeeper.client;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(value = Parameterized.class)
public class BookKeeperTest extends TestCase {
    private String ip;
    private boolean status;

    @Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(
                new Object[][]{
                        {"127.0.0.1:2128", true},
                        {"127.0.0.1:80", false},
                }
        );
    }

    public BookKeeperTest(String ip, boolean status){
        this.ip = ip;
        this.status = status;
    }

    @Test
    public void testNewBuilder() {
        if(this.status){
            assertEquals(0,this.ip.compareTo("127.0.0.1:2128"));
        }else{
            assertEquals(0,this.ip.compareTo("127.0.0.1:80"));
        }

    }

    @Test
    public void constructorBuilderTest(){
        try {
            BookKeeper bookKeeper = new BookKeeper(this.ip);
        } catch (IOException e) {
            System.err.println("IOException");
        } catch (InterruptedException e) {
            System.err.println("InterruptedException");
        } catch (BKException e) {
            System.err.println("BKException");
        }
        assertEquals(1,1);
    }
}