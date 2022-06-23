package org.apache.bookkeeper.bookie;


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class FileInfoTest {

    @Rule
    public TemporaryFolder folder=new TemporaryFolder();
    @Test
    public void testFileInfoClose()
    {
        try {
            File file= folder.newFile();
            byte [] masterkey=new byte[]{};
            FileInfo f= new FileInfo(file,masterkey,0);
            f.close(true);
            assertEquals(f.isClosed(),true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }
}
