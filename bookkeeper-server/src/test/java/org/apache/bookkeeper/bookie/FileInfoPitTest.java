package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.util.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
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
        MAX
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {ParamFile.RENAMEFAIL, ParamSize.MAX},

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
                File filee=createTempFile("FileTestFileInfo");
                this.fileInfo= new FileInfo(filee, "".getBytes(), 0);
                this.newFile = createTempFile("ciao2");
                this.exception=true;
                break;
        }
        switch (sizeType)
        {
            case MAX:
                this.size=Long.MAX_VALUE;
                break;
        }
    }
    @Test
    public void test(){
        try{
            fileInfo.moveToNewLocation(this.newFile,this.size);
            Assert.assertFalse(this.exception);
        } catch (IOException e) {
            Assert.assertTrue(this.exception);
        }

    }

}
