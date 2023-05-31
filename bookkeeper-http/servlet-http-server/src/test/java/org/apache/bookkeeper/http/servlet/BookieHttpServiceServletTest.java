package org.apache.bookkeeper.http.servlet;

import junit.framework.TestCase;
import org.apache.bookkeeper.http.NullHttpServiceProvider;
import org.junit.Before;
import org.mockito.Mockito;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import java.util.Enumeration;

import static org.mockito.Mockito.when;

public class BookieHttpServiceServletTest extends TestCase {
    private BookieHttpServiceServlet servletMock = Mockito.mock(BookieHttpServiceServlet.class);
    //@BeforeClass before all the tests

    @Before   //Start before each test case
    public void setUp() throws Exception {
        when(this.servletMock.getServletName()).thenReturn("api_v2");
        when(this.servletMock.getServletInfo()).thenReturn("api_v2");

    }

    public void tearDown() throws Exception {
    }

    public void testDoGet() {
    }

    public void testGetLastModified() {
    }

    public void testDoHead() {
    }

    public void testDoPost() {
    }

    public void testDoPut() {
    }

    public void testDoDelete() {
    }

    public void testDoOptions() {
    }

    public void testDoTrace() {
    }

    public void testService() {
    }

    public void testTestService() {
    }

    public void testDestroy() {
    }

    public void testGetInitParameter() {
    }

    public void testGetInitParameterNames() {
    }

    public void testGetServletConfig() {
    }

    public void testGetServletContext() {
    }

    public void testGetServletInfo() {
    }

    public void testInit() {
    }

    public void testTestInit() {
    }

    public void testLog() {
    }

    public void testTestLog() {
    }

    public void testGetServletName() {
    }

    public void testGetServletName2() {
        BookieHttpServiceServlet bookieHttpServiceServlet = new BookieHttpServiceServlet();

        //assertEquals(bookieHttpServiceServlet.getServletInfo(),this.servletMock.getServletInfo());
    }

    public void testTestService1() throws ServletException {
        BookieHttpServiceServlet bookieHttpServiceServlet = new BookieHttpServiceServlet();
        bookieHttpServiceServlet.init(new ServletConfig() {
            @Override
            public String getServletName() {
                return "api_v2";
            }

            @Override
            public ServletContext getServletContext() {
                return null;
            }

            @Override
            public String getInitParameter(String s) {
                return null;
            }

            @Override
            public Enumeration<String> getInitParameterNames() {
                return null;
            }
        });
        //assertEquals(this.servletMock.getServletName(),bookieHttpServiceServlet.getServletName());
    }

    public void testHttpServletParams() {
    }

    public void testHttpServerMethod() {
    }
}