package org.apache.bookkeeper.bookie;

import com.google.protobuf.compiler.PluginProtos;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class FileInfoTest {

    private FileInfo fileInfo;
    private File fileNew;
    private boolean exception;
    private String result;
    private long size;
    private int sizeExpected;
    enum ParamFile{
        NULL,CHIUSO,VUOTO
    }
    enum ParamSize{
        NEG,ZERO,MAX,MINOR
    }
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {ParamFile.NULL,ParamSize.MINOR},
                {ParamFile.CHIUSO,ParamSize.MINOR},
                {ParamFile.VUOTO,ParamSize.NEG},
                {ParamFile.VUOTO,ParamSize.ZERO},
                {ParamFile.VUOTO,ParamSize.MINOR},
                {ParamFile.VUOTO,ParamSize.MAX},
        });
    }
    void configure(ParamFile fileType,ParamSize sizeType) throws IOException {
        File fl= createTempFile("testFileInfo");
        this.result="testFileInfo1";
        String masterkey= "";
        this.fileInfo=new FileInfo(fl,masterkey.getBytes(),0);

        ByteBuffer bb[]=new ByteBuffer[1];
        bb[0]= ByteBuffer.wrap(this.result.getBytes());
        this.fileInfo.write(bb,0);

        this.exception=false;

        switch (fileType){
            case NULL:
                this.fileNew=null;
                this.exception=true;
                break;
            case VUOTO:
                this.fileNew=createTempFile("testFileInfoNewFile");
                break;
            case CHIUSO:
                this.fileNew=createTempFile("testFileInfoNewFile");
                this.fileNew.delete();
                this.exception=false;
                break;
        }
        switch (sizeType) {
            case NEG:
                this.size = -1;
                this.sizeExpected=0;
                this.exception = false;
                break;
            case ZERO:
                this.size = 1024;
                this.sizeExpected=0;
                break;
            case MINOR:
                this.sizeExpected=this.result.length()-1;
                this.size = 1024+this.result.length()- 1;
                break;
            case MAX:
                this.size = Long.MAX_VALUE;
                this.sizeExpected=this.result.length();
                break;
        }
    }
    public FileInfoTest(ParamFile fileType,ParamSize sizeType){
        try {
            configure(fileType,sizeType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private File createTempFile(String suffix) throws IOException {
        File file=IOUtils.createTempFileAndDeleteOnExit("bookie",suffix);
        return file;
    }
    @Test
   public void test1(){
        try{
            this.fileInfo.moveToNewLocation(this.fileNew, this.size);
            ByteBuffer b=ByteBuffer.allocate(this.sizeExpected+10);
            FileInfo s=new FileInfo(this.fileInfo.getLf(),"".getBytes(),0);
            int x=s.read(b,0,true);
            Assert.assertEquals(x,this.sizeExpected);
            for (int i = 0; i < x; i++) {
                Assert.assertEquals(result.charAt(i)+" "+b.array()[i],result.charAt(i), (char) b.array()[i]);
            }
            Assert.assertFalse(this.exception);

        }catch(Exception e)
        {
            Assert.assertTrue("mi aspettavo true"+e.getMessage(),this.exception);
        }

    }
}

