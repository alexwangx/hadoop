package org.apache.hadoop.hdfs.tools;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.FastCopy;
import org.apache.hadoop.hdfs.FastCopy.FastFileCopyRequest;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFastCopy {
  private Configuration conf = null;
  private MiniDFSCluster cluster = null;
  private DistributedFileSystem srcDFS = null;
  private DistributedFileSystem dstDFS = null;

  private long BLOCKSIZE = 1024;
  private short REPLICATION = 2;

  private Path file0 = new Path("/testFastCopy/NoBlocks");
  private Path file1 = new Path("/testFastCopy/TenBlocks");
  private Path file2 = new Path("/testFastCopy/sub/NoBlocks");
  private Path file3 = new Path("/testFastCopy/sub/TenBlocks");

  @Before
  public void setup() throws IOException {
    try {
      conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);

      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
              .numDataNodes(3).build();

      cluster.waitActive();

      srcDFS = cluster.getFileSystem(0);
      dstDFS = cluster.getFileSystem(1);

      DFSTestUtil.createFile(srcDFS, file0, 0, REPLICATION, 0L);
      DFSTestUtil.createFile(srcDFS, file1, 10*BLOCKSIZE, REPLICATION, 0L);
      DFSTestUtil.createFile(srcDFS, file2, 0, REPLICATION, 0L);
      DFSTestUtil.createFile(srcDFS, file3, 10*BLOCKSIZE, REPLICATION, 0L);

    } catch (Exception e) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @After
  public void shutdown() throws Exception {
    if(cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFastCopy() throws Exception {

    FastCopy fcp = new FastCopy(conf, 2, true);

    List<FastFileCopyRequest> requests = new ArrayList<FastFileCopyRequest>();

    requests.add(new FastFileCopyRequest(file0.toString(), file0.toString(),
            srcDFS, dstDFS));
    requests.add(new FastFileCopyRequest(file1.toString(), file1.toString(),
            srcDFS, dstDFS));
    requests.add(new FastFileCopyRequest(file2.toString(), file2.toString(),
            srcDFS, dstDFS));
    requests.add(new FastFileCopyRequest(file3.toString(), file3.toString(),
            srcDFS, dstDFS));
    fcp.copy(requests);
    fcp.shutdown();

    assertTrue(dstDFS.isFile(file0));
    assertTrue(dstDFS.isFile(file1));
    assertTrue(dstDFS.isFile(file2));
    assertTrue(dstDFS.isFile(file3));

    assertTrue(dstDFS.getFileChecksum(file0).equals(
            srcDFS.getFileChecksum(file0)));
    assertTrue(dstDFS.getFileChecksum(file1).equals(
            srcDFS.getFileChecksum(file1)));
    assertTrue(dstDFS.getFileChecksum(file2).equals(
            srcDFS.getFileChecksum(file2)));
    assertTrue(dstDFS.getFileChecksum(file3).equals(
            srcDFS.getFileChecksum(file3)));
  }

}