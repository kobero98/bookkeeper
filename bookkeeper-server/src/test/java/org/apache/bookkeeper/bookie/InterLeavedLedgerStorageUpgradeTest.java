package org.apache.bookkeeper.bookie;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.utils.TestBKConfiguration;
import org.apache.bookkeeper.client.utils.TestStatsProvider;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class InterLeavedLedgerStorageUpgradeTest {
    private Optional<RateLimiter> rateLimiter;
    private boolean exception ;
    private int sizeResult;

    TestStatsProvider statsProvider = new TestStatsProvider();
    ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
    LedgerDirsManager ledgerDirsManager;
    InterleavedLedgerStorage storage;
    final long numWrites = 10;
    final long entriesPerWrite = 2;
    final long numOfLedgers = 2;
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
               // {ParamOption.NULL,ConfigOption.EMPTY},
               // {ParamOption.EMPTY,ConfigOption.EMPTY},
               // {ParamOption.VALID,ConfigOption.EMPTY},

                //{ParamOption.NULL,ConfigOption.PRESENT},
               // {ParamOption.EMPTY,ConfigOption.PRESENT},
               // {ParamOption.VALID,ConfigOption.PRESENT},

        });
    }
    public enum ConfigOption{
        EMPTY,PRESENT
    }
    public enum ParamOption{
        NULL,EMPTY,VALID,INVALID
    }

    public InterLeavedLedgerStorageUpgradeTest(ParamOption option,ConfigOption setup){
        configure(option,setup);
    }

    public void configure(ParamOption option,ConfigOption setup){
        exception=false;
        switch (setup){
           case EMPTY:
               try {
                   setUp1();
               } catch (IOException e) {
                   throw new RuntimeException(e);
               }
               break;
           case PRESENT:
               try {
                   setUp2();
               } catch (IOException e) {
                   throw new RuntimeException(e);
               }
               break;
       }

        switch (option){
            case NULL:
                this.rateLimiter=null;
                if(setup==ConfigOption.PRESENT) exception=true;
                else {
                    exception = false;
                    sizeResult=0;
                }

                break;
            case EMPTY:
                this.rateLimiter=Optional.empty();
                exception=false;
                this.sizeResult=0;
                break;
            case INVALID:
                this.rateLimiter= Optional.of(mock(RateLimiter.class));
                exception=true;
                this.sizeResult=0;
                break;
            case VALID:
                this.rateLimiter= Optional.of(RateLimiter.create(1));
                this.sizeResult=0;
                break;
        }
    }

    public void setUp2() throws IOException {
        this.storage=new InterleavedLedgerStorage();
        CheckpointSource checkpointSource = new CheckpointSource() {
            @Override
            public Checkpoint newCheckpoint() {
                return Checkpoint.MAX;
            }

            @Override
            public void checkpointComplete(Checkpoint checkpoint, boolean compact) {
            }
        };
        Checkpointer checkpointer = new Checkpointer() {
            @Override
            public void startCheckpoint(CheckpointSource.Checkpoint checkpoint) {

            }

            @Override
            public void start() {
                // no-op
            }
        };
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);
        conf.setLedgerDirNames(new String[]{tmpDir.toString()});
        ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(TestBKConfiguration.newServerConfiguration());
        storage.initializeWithEntryLogger(
                conf, null, ledgerDirsManager, ledgerDirsManager, entryLogger,
                statsProvider.getStatsLogger(BOOKIE_SCOPE));
        storage.setCheckpointer(checkpointer);
        storage.setCheckpointSource(checkpointSource);
        // Insert some ledger & entries in the interleaved storage
        for (long entryId = 0; entryId < numWrites; entryId++) {
            for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
                if (entryId == 0) {
                    storage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
                    storage.setFenced(ledgerId);
                }
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId * entriesPerWrite);
                entry.writeBytes(("entry-" + entryId).getBytes());
                storage.addEntry(entry);

            }
        }
    }
    public void setUp1() throws IOException {
        this.storage=new InterleavedLedgerStorage();
        CheckpointSource checkpointSource = new CheckpointSource() {
            @Override
            public Checkpoint newCheckpoint() {
                return Checkpoint.MAX;
            }

            @Override
            public void checkpointComplete(Checkpoint checkpoint, boolean compact) {
            }
        };
        Checkpointer checkpointer = new Checkpointer() {
            @Override
            public void startCheckpoint(CheckpointSource.Checkpoint checkpoint) {

            }

            @Override
            public void start() {
                // no-op
            }
        };
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);
        conf.setLedgerDirNames(new String[]{tmpDir.toString()});
        ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(TestBKConfiguration.newServerConfiguration());
        storage.initializeWithEntryLogger(
                conf, null, ledgerDirsManager, ledgerDirsManager, entryLogger,
                statsProvider.getStatsLogger(BOOKIE_SCOPE));
        storage.setCheckpointer(checkpointer);
        storage.setCheckpointSource(checkpointSource);
    }


    @Test
    public void test1(){
        try {
            List<LedgerStorage.DetectedInconsistency> listInconsistency=storage.localConsistencyCheck(this.rateLimiter);
            Assert.assertEquals(listInconsistency.size(),this.sizeResult);
            Assert.assertFalse(this.exception);
        } catch (Exception e) {
            Assert.assertTrue(this.exception);
        }


    }
}
