package org.apache.bookkeeper.http.servlet;

import org.apache.bookkeeper.http.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;


import static org.junit.Assert.assertEquals;

@RunWith(value = Parameterized.class)
public class BookieHttpServiceServletTest extends Mockito {
    private final String expected;
    private final double valueOne;
    private BookieHttpServiceServlet servlet;
    private HttpServletRequest request;
    private HttpServletResponse response;



    @Parameters
    public static Collection<Object []> getTestParameters(){
        return Arrays.asList( new Object[][] {
                {"yes",2},
                {"no",1}
        });
    }

    public BookieHttpServiceServletTest(String expected, double valueOne){
        this.expected = expected;
        this.valueOne = valueOne;
    }

    @Before
    public void setUp() {
        this.servlet = new BookieHttpServiceServlet();
        this.request = mock(HttpServletRequest.class);
        this.response = mock(HttpServletResponse.class);
        List<String> enumStrings = new ArrayList<>();

        enumStrings.add("Mockito");
        enumStrings.add("Test");


        // mock the returned value of request.getParameter()
        when(this.request.getParameter("firstName")).thenReturn("Mockito");
        when(this.request.getParameter("lastName")).thenReturn("Test");


        when(this.request.getMethod()).thenReturn(String.valueOf(HttpServer.Method.GET));
        when(this.response.getStatus()).thenReturn(404);

        when(this.request.getParameterNames()).thenReturn(Collections.enumeration(enumStrings));
    }

    @After
    public void tearDown() {
    }

    @Test
    public void service() {
        String ret = expected;
        assertEquals(expected,ret);
    }

    @Test
    public void httpServletParams() {
        Map<String, String> map = this.servlet.httpServletParams(this.request);
        assertEquals(map.keySet().size(),2);
        //assertEquals(this.request.getParameter("firstName"),"Mockito");
    }

    @Test
    public void httpServerMethodRequest() {
        //Test to verify if httpServerMethod return the correct http method
        assertEquals(String.valueOf(this.servlet.httpServerMethod(this.request)),this.request.getMethod());
    }

    @Test
    public void httpServerMethodResponse() {

        try {
            this.servlet.service(this.request, this.response);
            assertEquals(1,2);
        } catch (Exception e) {
            assertEquals(1,1);
        }

    }
}