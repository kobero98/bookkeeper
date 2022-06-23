package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.PortManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class JournalTest {

    @Rule
    public TemporaryFolder folder= new TemporaryFolder();


    @Test
    public void  testcheckPointComplete() throws IOException {
        File journalDir = folder.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
        ServerConfiguration conf = newServerConfiguration()
                .setJournalDirName(journalDir.getPath())
                .setMetadataServiceUri(null)
                .setJournalAdaptiveGroupWrites(false);

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);

        Journal journal = new Journal(0, journalDir, conf, ledgerDirsManager);
        List<Long> ids=Journal.listJournalIds(journalDir,null);

        CheckpointSource.Checkpoint c= mock(CheckpointSource.Checkpoint.class);
        journal.checkpointComplete(c,false);

        assertEquals(0, ids.size());

    }


}
