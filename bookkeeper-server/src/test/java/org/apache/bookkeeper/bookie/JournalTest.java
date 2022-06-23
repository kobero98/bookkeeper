package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
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
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

//@RunWith(Parameterized.class)
public class JournalTest {

    @Rule
    public TemporaryFolder folder= new TemporaryFolder();
    static final Logger LOG = LoggerFactory.getLogger(JournalTest.class);
    private static String getLoopbackInterfaceName() {
        try {
            Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface nif : Collections.list(nifs)) {
                if (nif.isLoopback()) {
                    return nif.getName();
                }
            }
        } catch (SocketException se) {
            LOG.warn("Exception while figuring out loopback interface. Will use null.", se);
            return null;
        }
        LOG.warn("Unable to deduce loopback interface. Will use null");
        return null;
    }
    public static ServerConfiguration setLoopbackInterfaceAndAllowLoopback(ServerConfiguration serverConf) {
        serverConf.setListeningInterface(getLoopbackInterfaceName());
        serverConf.setAllowLoopback(true);
        return serverConf;
    }
    public static ServerConfiguration newServerConfiguration() {
        ServerConfiguration confReturn = new ServerConfiguration();
        confReturn.setTLSEnabledProtocols("TLSv1.2,TLSv1.1");
        confReturn.setJournalFlushWhenQueueEmpty(true);
        // enable journal format version
        confReturn.setJournalFormatVersionToWrite(5);
        confReturn.setAllowEphemeralPorts(false);
        confReturn.setBookiePort(PortManager.nextFreePort());
        confReturn.setGcWaitTime(1000);
        confReturn.setDiskUsageThreshold(0.999f);
        confReturn.setDiskUsageWarnThreshold(0.99f);
        confReturn.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        confReturn.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 4);
        confReturn.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 4);
        /**
         * if testcase has zk error,just try 0 time for fast running
         */
        confReturn.setZkRetryBackoffMaxRetries(0);
        setLoopbackInterfaceAndAllowLoopback(confReturn);
        return confReturn;
    }

    @Test
    public void  checkPointCompleteTest() throws IOException {
        File journalDir = folder.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
        ServerConfiguration conf = newServerConfiguration()
                .setJournalDirName(journalDir.getPath())
                .setMetadataServiceUri(null)
                .setJournalAdaptiveGroupWrites(false);
        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        Journal journal = new Journal(0, journalDir, conf, ledgerDirsManager);
        CheckpointSource.Checkpoint c= mock(CheckpointSource.Checkpoint.class);
        journal.checkpointComplete(c,false);
        List<Long> ids=Journal.listJournalIds(journalDir,null);
        assertEquals(0, ids.size());

    }


}
