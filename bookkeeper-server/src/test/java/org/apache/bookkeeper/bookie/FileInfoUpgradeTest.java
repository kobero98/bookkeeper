package org.apache.bookkeeper.bookie;

import com.google.protobuf.compiler.PluginProtos;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.*;
import java.nio.ByteBuffer;
import java.sql.ParameterMetaData;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class FileInfoUpgradeTest {

    private FileInfo fileInfo;
    private File fileNew;
    private boolean exception;
    private String result;
    private long size;
    private int sizeExpected;
    private boolean deleted;

    enum ParamFile {
        NULL, CHIUSO, VUOTO, SAME, FC_NULL
    }

    enum ParamSize {
        NEG, ZERO, MAX, MINOR
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {ParamFile.SAME, ParamSize.MAX},
                {ParamFile.FC_NULL, ParamSize.MAX}
        });
    }

    void configure(ParamFile fileType, ParamSize sizeType) throws IOException {
        File fl = createTempFile("testFileInfo");
        this.result = "testFileInfo1";
        String masterkey = "";
        this.fileInfo = new FileInfo(fl, masterkey.getBytes(), 0);

        this.deleted = false;
        this.exception = false;
        switch (fileType) {
            case SAME:
                this.fileNew = fl;
                ByteBuffer bb[] = new ByteBuffer[1];
                bb[0] = ByteBuffer.wrap(this.result.getBytes());
                this.fileInfo.write(bb, 0);
                break;
            case FC_NULL:
                File f = mock(File.class);
                Mockito.when(f.delete()).thenReturn(true);
                Mockito.when(f.getPath()).thenReturn("/tmp/123456789");
                this.fileInfo = new FileInfo(f, "".getBytes(), 0);
                this.fileNew = createTempFile("ciao2");
                break;
        }
        switch (sizeType) {
            case MAX:
                this.size = Long.MAX_VALUE;
                this.sizeExpected = this.result.length();
                break;
        }
    }

    public FileInfoUpgradeTest(ParamFile fileType, ParamSize sizeType) {
        try {
            configure(fileType, sizeType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private File createTempFile(String suffix) throws IOException {
        File file = IOUtils.createTempFileAndDeleteOnExit("bookie", suffix);
        return file;
    }

    @Test
    public void test1() {
        try {
            this.fileInfo.moveToNewLocation(this.fileNew, this.size);
            Assert.assertEquals(this.deleted, fileInfo.isDeleted());

        } catch (Exception e) {
            Assert.assertTrue("mi aspettavo true " + e.getMessage(), this.exception);
        }
    }
}