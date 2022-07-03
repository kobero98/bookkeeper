package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.client.utils.TestBKConfiguration;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.parameterized.ParametersRunnerFactory;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class BookieImplTest {
    private ServerConfiguration configuration;
    private boolean exception;
    private final List<File> tempDirs = new ArrayList<>();
    private String resultExpected;

    public enum ParamConfServer{
        NULL,INVALID,ABSTRACT,EMPTY,
    }
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {ParamConfServer.NULL},
                {ParamConfServer.EMPTY},
                {ParamConfServer.ABSTRACT},
             });
    }
    private void configuration(ParamConfServer conf){
        this.exception=false;

        switch (conf)
        {
            case NULL:
                configuration=null;
                this.exception=true;
                break;
            case EMPTY:
                configuration=new ServerConfiguration();
                this.exception=false;
                try {
                    this.resultExpected= Inet4Address.getLocalHost().getHostAddress();
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }

                break;
            case ABSTRACT:
                AbstractConfiguration s= new ServerConfiguration();
                configuration=new ServerConfiguration(s);
                this.exception=false;
                try {
                    this.resultExpected= Inet4Address.getLocalHost().getHostAddress();
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                break;
            case INVALID:
                break;

        }
    }

    public BookieImplTest(ParamConfServer conf){
        configuration(conf);
    }
    private File createTempDir(String suffix) throws IOException {
        File dir = IOUtils.createTempDir("bookie", suffix);
        tempDirs.add(dir);
        return dir;
    }
    @Test
    public void test(){
        try{
        BookieSocketAddress a= BookieImpl.getBookieAddress(this.configuration);
        Assert.assertEquals(a.toString(),this.resultExpected,a.getHostName());
        Assert.assertFalse("mi aspettavo False",this.exception);
            }
        catch (Exception e)
        {
            Assert.assertTrue("mi aspettavo true",this.exception);
        }
    }

}
