package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.util.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.mock;
@RunWith(Parameterized.class)
public class FileInfoPitTest {
    private File createTempFile(String suffix) throws IOException {
        File file = IOUtils.createTempFileAndDeleteOnExit("bookie", suffix);
        return file;
    }


    enum ParamFile {
        RENAMEFAIL
    }

    enum ParamSize {
        ZERO
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {ParamFile.RENAMEFAIL, ParamSize.ZERO},

        });
    }
    public FileInfoPitTest(ParamFile filetype, ParamSize sizeType){
        try {
            configure(filetype,sizeType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private boolean exception;
    private FileInfo fileInfo;
    private File newFile;
    private Long size;

    public void configure(ParamFile filetype, ParamSize sizeType) throws IOException {
        switch (filetype){
            case RENAMEFAIL:
                File app=createTempFile("FileInfoTest");
                this.fileInfo=new FileInfo(app,"".getBytes(),0);
                this.newFile=createTempFile("newFileInfoTest");
                this.exception=false;
                break;
        }
        switch (sizeType)
        {
            case ZERO:
                this.size= Long.valueOf(0);
                break;
        }
    }
    @Test
    public void test(){
        try{
            fileInfo.moveToNewLocation(this.newFile,this.size);
            Assert.assertFalse("mi aspettavo true",this.exception);
        } catch (IOException e) {
            Assert.assertTrue("mi aspettavo false",this.exception);
        }

    }

}
