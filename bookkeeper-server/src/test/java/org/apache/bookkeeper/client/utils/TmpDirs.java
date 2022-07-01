package org.apache.bookkeeper.client.utils;


import java.io.File;
import java.util.LinkedList;
import java.util.List;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;

/**
 * Utility class for managing tmp directories in tests.
 */
public class TmpDirs {
    private final List<File> tmpDirs = new LinkedList<>(); // retained to delete files

    public File createNew(String prefix, String suffix) throws Exception {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tmpDirs.add(dir);
        return dir;
    }

    public void cleanup() throws Exception {
        for (File f : tmpDirs) {
            FileUtils.deleteDirectory(f);
        }
    }

    public List<File> getDirs() {
        return tmpDirs;
    }
}
