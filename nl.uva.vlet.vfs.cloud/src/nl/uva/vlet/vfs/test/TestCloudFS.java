package nl.uva.vlet.vfs.test;

import java.io.*;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import static nl.uva.vlet.data.VAttributeConstants.ATTR_EXISTS;
import static nl.uva.vlet.data.VAttributeConstants.ATTR_HOSTNAME;
import static nl.uva.vlet.data.VAttributeConstants.ATTR_LENGTH;
import static nl.uva.vlet.data.VAttributeConstants.ATTR_LOCATION;
import static nl.uva.vlet.data.VAttributeConstants.ATTR_MIMETYPE;
import static nl.uva.vlet.data.VAttributeConstants.ATTR_NAME;
import static nl.uva.vlet.data.VAttributeConstants.ATTR_PATH;
import static nl.uva.vlet.data.VAttributeConstants.ATTR_PORT;
import static nl.uva.vlet.data.VAttributeConstants.ATTR_SCHEME;
import static nl.uva.vlet.data.VAttributeConstants.ATTR_TYPE;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import junit.framework.Assert;
import nl.uva.vlet.ClassLogger;
import nl.uva.vlet.exception.ResourceAlreadyExistsException;
import nl.uva.vlet.exception.ResourceCreationFailedException;
import nl.uva.vlet.exception.VRLSyntaxException;
import nl.uva.vlet.exception.VlException;
import nl.uva.vlet.io.CircularStreamBufferTransferer;
import nl.uva.vlet.vfs.VDir;
import nl.uva.vlet.vfs.VFSClient;
import nl.uva.vlet.vfs.VFSNode;
import nl.uva.vlet.vfs.VFile;
import nl.uva.vlet.vfs.VFileSystem;
import nl.uva.vlet.vfs.cloud.CloudFSFactory;
import nl.uva.vlet.vfs.cloud.CloudFileSystem;
import nl.uva.vlet.vfs.cloud.Exceptions.CloudRequestTimeout;
import nl.uva.vlet.vrl.VRL;
import nl.uva.vlet.vrs.ServerInfo;
import nl.uva.vlet.vrs.VRS;
import nl.uva.vlet.vrs.VRSContext;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.util.EntityUtils;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.rest.RestContext;

public class TestCloudFS {

    private static VRL testLoc;
    static ClassLogger logger;

    static {
        logger = ClassLogger.getLogger(TestCloudFS.class);
        logger.setLevelToDebug();
    }

    static {
        try {
//            testLoc = new VRL(
//                    "swift://149.156.10.131:8443/auth/v1.0/TEST_VRS_LOC");

            testLoc = new VRL(
                    "swift://149.156.10.131:8443/auth/v1.0/testBlobStoreVFS");


//            testLoc = new VRL(
//                    "filesystem:/testBlobStoreVFS");
        } catch (VRLSyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    private static VDir testRemoteDir;
    private static VDir localTempDir;
    private static VRL localTempDirVrl = TestSettings.getTestLocation(TestSettings.VFS_LOCAL_TEMPDIR_LOCATION);
    private static String uname;
    private static String key;

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            setUp();

            // testOpenLocation();
            // testNewNode();
            // testOpenNode();
            // testGetNrOfNodes();
            // testCreateDir();

            // testGetModificationTime();

            // testDelete();

            // testList();

            // testGetInStream();

//            testGetOutStream();

            // testGetContLen();

            // testToch();

            // testListDirFiltered();

//			testExists();

            // testCreateAndDeleteFile();

            // testCreateDirectoryWhileFileWithSameNameExists();

            // testExceptionsExistingDir();

            // testCreateAndIgnoreExistingDir();

            // testSetGetSimpleContentsNewFile();

            // testFileAttributes();

            // testZCreateFileWhileDirectoryWithSameNameExists();

//            measureUpload();

//            testSwiftCloudOutputStream();

            testMove10MBForthAndBack();

//            testGetInputStream();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            VRS.exit();
            System.exit(0);
        }

    }

    private static void testToch() throws InterruptedException,
            ExecutionException, VlException {
        VRL vrl = testLoc.append("/test_1/deleteMe/dir1/FILE2");

        boolean exists = cloudFS.openLocation(vrl).exists();

        if (exists) {
            cloudFS.rm(vrl, StorageType.BLOB);
        }

        cloudFS.touch(vrl, true);
        exists = cloudFS.openLocation(vrl).exists();
        logger.debugPrintf("%s exists?? %s\n", vrl, exists);
    }

    private static void testGetContLen() throws VlException {
        VRL vrl = testLoc.append("/test_1/deleteMe/dir1/FILE1");

        long len = ((VFile) cloudFS.openLocation(vrl)).getLength();

        logger.debugPrintf("File len: %s\n", len);

    }

    private static void testGetOutStream() throws VRLSyntaxException,
            IOException, InterruptedException, ExecutionException, CloudRequestTimeout {
        VRL vrl = testLoc.append("/test_1/deleteMe/dir1/FILE1");

        OutputStream out = cloudFS.getOutputStream(vrl);

        out.write(">>>>>>>>>>>>>>>>>>DATA>>>>>>>>>>>".getBytes());

        out.flush();

        out.close();

        InputStream ins = cloudFS.getInputStream(vrl);

        int b;
        byte[] data = new byte[512];
        while ((b = ins.read(data)) != -1) {
            logger.debugPrintf("Data: %s\n", new String(data));
        }

        ins.close();

        // // Small stress test
        // out = cloudFS.getOutputStream(vrl);
        //
        // Random r = new Random();
        //
        // byte[] bigArray = new byte[1024 * 1024];
        //
        // long start = System.currentTimeMillis();
        //
        // long dataSize = 0;
        // for (int i = 0; i < 200; i++) {
        // r.nextBytes(bigArray);
        // out.write(bigArray);
        //
        // dataSize = dataSize + bigArray.length + i;
        //
        // logger.debugPrintf("Writing %s MB\n", (dataSize/(1024.0*1024.0)));
        // }
        //
        // out.flush();
        // out.close();
        //
        // long end = System.currentTimeMillis();
        // long elapsed = (long) ((end - start)/1000.0);
        //
        // logger.debugPrintf("Wrote %s MB/sec \n",
        // (dataSize/(1024.0*1024.0))/elapsed);
    }

    private static void testGetInStream() throws VRLSyntaxException,
            IOException, InterruptedException, ExecutionException, CloudRequestTimeout {
        VRL vrl = testLoc.append("/test_3/deleteMe/dir1/FILE");

        InputStream ins = cloudFS.getInputStream(vrl);

        byte[] data = new byte[1024];
        ins.read(data);

        logger.debugPrintf("Got data: %s\n", new String(data));
        ins.close();

        vrl = testLoc.append("/test_1/2M.dat");
        ins = cloudFS.getInputStream(vrl);

        data = new byte[1024];
        ins.read(data);

        logger.debugPrintf("Got data: %s\n", new String(data));
        ins.close();
    }

    private static void testList() throws VRLSyntaxException,
            InterruptedException, ExecutionException, CloudRequestTimeout {
        // VRL vrl = testLoc.append("/test_2/newDir");
        VFSNode[] nodes = cloudFS.ls(testLoc);
        for (VFSNode n : nodes) {
            logger.debugPrintf("ls: %s\n", n.getVRL());
        }
    }

    private static void testDelete() throws VlException, InterruptedException,
            ExecutionException {

        VRL vrl = testLoc.append("/test_1/newDir1");
        // cloudFS.createDir(vrl, true);
        boolean success = cloudFS.rm(vrl, StorageType.FOLDER);

        logger.debugPrintf("Success?? %s \n", success);

        // vrl = testLoc.append("/test_3/deleteMe/dir1/FILE");
        // cloudFS.createFile(vrl, true);
        // cloudFS.rm(vrl);

    }

    private static void testGetModificationTime() throws VlException {

        long modTime = cloudFS.openLocation(testLoc.append("/test_1/aa/")).getModificationTime();

        cloudFS.openLocation(testLoc.append("/test_1")).getModificationTime();

        cloudFS.openLocation(testLoc.append("/test_1/newDir")).getModificationTime();

        cloudFS.openLocation(testLoc.append("/test_1/aa/bb/2M.dat")).getModificationTime();

    }

    private static void testGetNrOfNodes() throws VRLSyntaxException,
            InterruptedException, ExecutionException, CloudRequestTimeout {
        long num = cloudFS.getNumOfNodes(testLoc.append("test_1"));
        logger.debugPrintf("%s  Num of nodes: %s\n", testLoc.append("test_1"),
                num);

        // cloudFS.getNumOfNodes(testLoc.append("test_2/dir_1"));
        // logger.debugPrintf("Num of nodes: %s\n", num);

        num = cloudFS.getNumOfNodes(testLoc.append("/test_1/aa/bb/2M.dat"));
        logger.debugPrintf("%s  Num of nodes: %s\n",
                testLoc.append("/test_1/aa/bb/"), num);
    }

    private static void testCreateDir() throws VlException,
            InterruptedException, ExecutionException {
        // VRL vrl1 = testLoc.append("test_1/newDir1");
        // cloudFS.createDir(testLoc.append("test_1/newDir1"), false);

        // cloudFS.rm(vrl1);

        VRL vrl2 = testLoc.append("/testBlobStoreVFS");
        cloudFS.createDir(testLoc.append("testBlobStoreVFS"), false);

        cloudFS.rm(vrl2, StorageType.FOLDER);
    }

    private static void testOpenNode() throws VlException {
        cloudFS.openDir(testLoc.append("test_1"));
        cloudFS.openDir(testLoc.append("test_3"));

        cloudFS.openFile(testLoc.append("test_1/200M.dat"));

    }

    private static void testNewNode() throws VlException {
        cloudFS.newDir(testLoc.append("test_4"));

        cloudFS.newFile(testLoc.append("test_4/file"));
    }

    private static void testOpenLocation() throws VlException {
        VFSNode node = cloudFS.openLocation(testLoc);
        node = cloudFS.openLocation(testLoc.append("test_1"));
        node = cloudFS.openLocation(testLoc.append("test_2"));
        node = cloudFS.openLocation(testLoc.append("test_2/dir_1"));

        node = cloudFS.openLocation(testLoc.append("test_3"));

        // logger.debugPrintf("VNode: %s \n", node.getVRL());

        node = cloudFS.openLocation(testLoc.append("test_1/200M.dat"));
        // logger.debugPrintf("VNode: %s \n", node.getVRL());

        cloudFS.openLocation(testLoc.append("test_2/dir_1"));

        node = cloudFS.openLocation(testLoc.append("test_1/a/b/c/d/e/f/g/h/test"));

        node = cloudFS.openLocation(testLoc.append("test_1/aa/bb/2M.dat"));
    }
    private static CloudFileSystem cloudFS;

    public static void setUp() throws Exception {
        VRS.getRegistry().addVRSDriverClass(CloudFSFactory.class);

        VRSContext context = VFSClient.getDefault().getVRSContext();

        context.setGridProxy(null);

        ServerInfo info = context.getServerInfoFor(testLoc, true);

        // Set Authentication information
        info.setUsePasswordAuth();

        info.setAuthScheme(ServerInfo.PASSWORD_OR_PASSPHRASE_AUTH);


        uname = getCloudProperties().getProperty("jclouds.identity");
        key = getCloudProperties().getProperty("jclouds.credential");


        info.setUsername(uname);
        info.setPassword(key);
        info.store();

        CloudFSFactory fsFactory = new CloudFSFactory();

        cloudFS = (CloudFileSystem) fsFactory.createNewFileSystem(context,
                info, testLoc);

        testRemoteDir = cloudFS.createDir(testLoc, true);

        localTempDir = new VFSClient().createDir(localTempDirVrl, true);

    }

    public static void testListDirFiltered() throws VlException {

        logger.debugPrintf(">>>> createDir\n");
        VDir ldir = getRemoteTestDir().createDir("dirListTest");

        logger.debugPrintf("createDir(dirListTest) \n");

        // list EMPTY dir:
        logger.debugPrintf(">>>> list\n");
        VFSNode[] nodes = ldir.list();

        if ((nodes != null) && (nodes.length > 0)) {
            // previous junit test was aborted.
            try {
                ldir.delete(true);
            } catch (Exception e) {
                ;
            }
            Assert.fail("Pre condition failed: New created directory must be empty. Please Run junit test again");
        }

        try {
            ldir.createFile("file0");
            ldir.createFile("file1.txt");
            ldir.createFile("file2.aap");
            ldir.createFile("file3.aap.txt");

            // check plain list():
            VFSNode[] result = ldir.list();
            Assert.assertNotNull("List result may not be null", result);
            Assert.assertEquals("Number of returned files is not correct.", 4,
                    result.length);

            result = ldir.list("*", false);
            Assert.assertNotNull("List result may not be null", result);
            Assert.assertEquals("Number of returned files is not correct.", 4,
                    result.length);

            System.out.println("nr of filtered files '*' =" + result.length);

            result = ldir.list("*.txt", false);
            Assert.assertNotNull("List result may not be null", result);
            Assert.assertEquals("Number of returned files is not correct.", 2,
                    result.length);

            System.out.println("nr of filtered files '*.txt' =" + result.length);

            // test RE version of *.txt
            result = ldir.list(".*\\.txt", true);
            Assert.assertNotNull("List result may not be null", result);
            Assert.assertEquals("Number of returned files is not correct.", 2,
                    result.length);

            System.out.println("nr of filtered files '.*\\.txt' ="
                    + result.length);

        } finally {
            ldir.delete(true);
        }
    }

    public static void testExists() throws VlException {
        boolean result = getRemoteTestDir().existsFile(
                "ThisFileShouldnotexist_1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ");
        logger.debugPrintf("Exists():%s\n", result);

        result = getRemoteTestDir().existsDir(
                "ThisDirShouldnotexist_1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ");
        Assert.assertFalse("Exists(): Dir should not exist!", result);

        result = getRemoteTestDir().exists();
        Assert.assertTrue(
                "Exists(): *** ERROR: Remote Test directory doesn't exists. Tests will fail",
                result);

        if (true) {
            result = getRemoteTestDir().existsFile(
                    "This File Should not exist 1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ");
            Assert.assertFalse("Exists(): file should not exist!", result);

            result = getRemoteTestDir().existsDir(
                    "This Dir Should not exist 1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ");
            Assert.assertFalse("Exists(): Dir should not exist!", result);
        }
    }

    private static VDir getRemoteTestDir() throws VlException {
        VDir testDir = (VDir) cloudFS.openLocation(testLoc);
        return testDir;
    }

    private static void testCreateAndDeleteFile() throws VlException {

        VFile newFile = getRemoteTestDir().createFile(nextFilename("testFile"));
        // sftp created 1 byte length new files !
        Assert.assertNotNull("New created file may not be NULL", newFile);
        Assert.assertTrue("Length of newly created file must be 0!:"
                + getRemoteTestDir(), newFile.getLength() == 0);
        newFile.delete();

        // ---
        // Use newFile().create() method
        // ---
        Assert.assertFalse(
                "After deletion, a file may NOT report it still 'exists'!",
                newFile.exists());
        newFile = getRemoteTestDir().newFile("testFile1b");
        newFile.create(); // use default creat();

        // sftp created 1-length new files !
        Assert.assertNotNull("New created file may not be NULL", newFile);
        Assert.assertTrue("Length of newly created file must be 0!:"
                + getRemoteTestDir(), newFile.getLength() == 0);

        newFile.delete();
        Assert.assertFalse(
                "After deletion, a file may NOT report it still 'exists'!",
                newFile.exists());
    }
    private static Object uniquepathnrMutex = new Object();
    private static int uniquepathnr = 0;

    public static String nextFilename(String prefix) {
        synchronized (uniquepathnrMutex) {
            return prefix + uniquepathnr++;
        }
    }

    public static void testCreateAndIgnoreExistingDir() throws VlException {
        String dirname = "testDirG";

        VDir newDir = getRemoteTestDir().createDir(dirname);

        // current default implemenation is to ignore existing directories!
        newDir = getRemoteTestDir().createDir(dirname);

        try {
            newDir = getRemoteTestDir().createDir(dirname, true);
        } catch (Exception e) {
            Assert.fail("Caught Exception: When setting ignoreExisting==true. Method createDir() must ignore the already existing directory");
        }

        newDir.delete();
    }
    private static final String TEST_CONTENTS = ">>> This is a testfile used for the VFS unit tests  <<<\n"
            + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\n"
            + "0123456789@#$%*()_+\n"
            + "Strange characters:...<TODO>\n"
            + "UTF8:<TODO>\n" + "\n --- You Can Delete this File ---\n";

    public static void testSetGetSimpleContentsNewFile() throws VlException {
        // test1: small string
        VFileSystem fs = getRemoteTestDir().getFileSystem();
        VFile newFile = fs.newFile(getRemoteTestDir().resolvePathVRL(
                "testFile7"));

        newFile.setContents(TEST_CONTENTS);

        long newLen = newFile.getLength();
        Assert.assertFalse("After setting contents, size may NOT be zero",
                newLen == 0);

        String str = newFile.getContentsAsString();
        Assert.assertEquals("Contents of file (small string) not the same:"
                + str, str, TEST_CONTENTS);

        newFile.delete();

        //
        // recreat file:
        newFile = fs.newFile(getRemoteTestDir().resolvePathVRL("testFile7a"));

        int len = 1024 * 1024 + 1024;

        char chars[] = new char[len];

        for (int i = 0; i < len; i++) {
            chars[i] = (char) ('A' + (i % 26));
        }

        String bigString = new String(chars);

        newFile.setContents(bigString);
        str = newFile.getContentsAsString();

        if (str.compareTo(bigString) != 0) {
            String infoStr = "strlen=" + bigString.length() + ",newstrlen="
                    + str.length();

            Assert.fail("Contents of file (big string) not the same, but small string does!.\n"
                    + "info=" + infoStr);
        }

        newFile.delete();
    }

    public static void testFileAttributes() throws VlException {
        VFile newFile = getRemoteTestDir().newFile(nextFilename("testFileAttr"));

        testVFSNodeAttributes(newFile);

        newFile.create();
        testVFSNodeAttributes(newFile);

        newFile.delete();
    }

    private static void testVFSNodeAttributes(VFSNode newFile)
            throws VlException {

        Assert.assertEquals(
                "Both getType() and getAttribute(ATTR_TYPE) must return same value",
                newFile.getType(), newFile.getAttribute(ATTR_TYPE).getValue());

        Assert.assertEquals(
                "Both getName() and getAttribute(ATTR_NAME) must return same value",
                newFile.getName(), newFile.getAttribute(ATTR_NAME).getValue());

        Assert.assertEquals(
                "Both getVRL() and getAttribute('location') must return same value",
                newFile.getVRL().toString(), newFile.getAttribute(ATTR_LOCATION).getValue());

        // for hostname comparisons:
        Assert.assertEquals(
                "Both getHostname() and getAttribute(ATTR_HOSTNAME) must return same value",
                newFile.getHostname(), newFile.getAttribute(ATTR_HOSTNAME).getValue());

        // for hostname:port comparisons:
        Assert.assertEquals(
                "Both getPort() and getAttribute(ATTR_PORT) must return same value",
                newFile.getPort(), newFile.getAttribute(ATTR_PORT).getIntValue());

        // for scheme://hostname:port comparisons:
        Assert.assertEquals(
                "Both getScheme() and getAttribute(ATTR_SCHEME) must return same value",
                newFile.getScheme(), newFile.getAttribute(ATTR_SCHEME).getValue());

        // for scheme://hostname:port comparisons:
        Assert.assertEquals(
                "Both getPath() and getAttribute(ATTR_PATH) must return same value",
                newFile.getPath(), newFile.getAttribute(ATTR_PATH).getValue());

        Assert.assertEquals(
                "Both getMimetype() and getAttribute(ATTR_MIMETYPE) must return same value",
                newFile.getMimeType(), newFile.getAttribute(ATTR_MIMETYPE).getValue());

        Assert.assertEquals(
                "Both exists() and getAttribute(ATTR_EXISTS) must return same value",
                newFile.exists(), newFile.getAttribute(ATTR_EXISTS).getBooleanValue());

        if (newFile.exists()) {
            if (newFile.isFile()) {
                VFile file = (VFile) newFile;
                Assert.assertEquals(
                        "Both getLength() and getAttribute(ATTR_LENGTH) must return same value",
                        file.getLength(), newFile.getAttribute(ATTR_LENGTH).getLongValue());
            }
        }
    }

    public static void testZCreateFileWhileDirectoryWithSameNameExists()
            throws VlException {
        VDir newDir = getRemoteTestDir().createDir("testfiledir3");

        // MUST return false!
        Assert.assertFalse(
                "existsFile() must return FALSE when directory with same name already exists!",
                getRemoteTestDir().existsFile("testfiledir3"));
        try {
            VFile newfile = getRemoteTestDir().createFile("testfiledir3");
            Assert.fail("Create file out of existing directory should raise Exception:");
        } // both are allowed:
        catch (ResourceCreationFailedException e) {
        } catch (ResourceAlreadyExistsException e) {
        }
        {
            newDir.delete();
        }
    }

    private static void testSwiftCloudOutputStream() throws VlException, IOException {
        VFileSystem fs = getRemoteTestDir().getFileSystem();
        VFile newFile = fs.newFile(getRemoteTestDir().resolvePathVRL("testFile7"));
        try {

            byte[] writeBuffer = new byte[1024];
            //            Random r = new Random();
//            r.nextBytes(writeBuffer);
            for (int i = 0; i < writeBuffer.length; i++) {
                writeBuffer[i] = (byte) i;
            }

            OutputStream os = newFile.getOutputStream();
            os.write(writeBuffer);
            os.close();

            if (writeBuffer == null) {
                System.err.println("writeBuffer=NULL");
            }

            byte[] readBuffer = new byte[1024];
            InputStream is = newFile.getInputStream();
            is.read(readBuffer);
            is.close();

            if (readBuffer == null) {
                System.err.println("readBuffer=NULL");
            }


            for (int i = 0; i < readBuffer.length; i++) {
//                if (writeBuffer[i] != readBuffer[i]) {
//                    Assert.fail(writeBuffer[i] + " != " + readBuffer[i]);
//                }
                Assert.assertEquals(writeBuffer[i], readBuffer[i]);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            newFile.delete();
        }

    }

    private static void testMove10MBForthAndBack() throws Exception {


        VFile localFile = null;
        VFile remoteFile = null;
        VFile newLocalFile = null;
        {
            try {
                localFile = localTempDir.createFile("test10MBmove");
                int len = 50;

                byte buffer[] = new byte[len];
                for (int i = 0; i < buffer.length; i++) {
                    buffer[i] = (byte) (i + 1);
                }
                System.out.println("streamWriting to localfile:" + localFile);
                OutputStream out = localFile.getOutputStream();
                for (int i = 0; i < (1024 * 1024); i++) {
                    out.write(buffer);
                }
                out.flush();
                out.close();
                len = (int) localFile.getLength();
                InputStream ins = localFile.getInputStream();
                int rLen = 0;
                int writeCount = 0;
                while ((rLen = ins.read(buffer)) != -1) {
                    for (int i = 0; i < rLen; i++) {
                        if (buffer[i] == 1) {
                            writeCount++;
                        }
//                        System.out.println("WriteLocal: " + writeCount + " " + buffer[i]);
                    }
                }

                // move to remote (and do same basic asserts).
                long start_time = System.currentTimeMillis();
                System.out.println("moving localfile to:" + getRemoteTestDir());
                remoteFile = localFile.copyTo(getRemoteTestDir());
                long total_millis = System.currentTimeMillis() - start_time;
                double up_speed = (len / 1024.0) / (total_millis / 1000.0);
                System.out.println("upload speed=" + ((int) (up_speed * 1000)) / 1000.0
                        + "KB/s");


                if (localFile.getLength() != remoteFile.getLength()) {
                    System.err.println("Expected: " + localFile.getLength() + " got: " + remoteFile.getLength());
                    throw new Exception("Expected: " + localFile.getLength() + " got: " + remoteFile.getLength());
                }

                start_time = System.currentTimeMillis();

                newLocalFile = localTempDir.createFile("test10MBback");
                CircularStreamBufferTransferer csbt = new CircularStreamBufferTransferer(1024, remoteFile.getInputStream(), newLocalFile.getOutputStream());
                csbt.startTransfer(-1);


                total_millis = System.currentTimeMillis() - start_time;

                double down_speed = (len / 1024.0) / (total_millis / 1000.0);
                System.out.println("download speed=" + ((int) (down_speed * 1000)) / 1000.0
                        + "KB/s");

                if (newLocalFile.getLength() != remoteFile.getLength()) {
                    System.err.println("Expected: " + newLocalFile.getLength() + " got: " + remoteFile.getLength());
                    throw new Exception("Expected: " + newLocalFile.getLength() + " got: " + remoteFile.getLength());
                }

                if (newLocalFile.getLength() != localFile.getLength()) {
                    System.err.println("Expected: " + localFile.getLength() + " got: " + newLocalFile.getLength());
                    throw new Exception("Expected: " + localFile.getLength() + " got: " + newLocalFile.getLength());
                }

//                ins = newLocalFile.getInputStream();
//                int rRen = 0;
//                int readDowncount = 0;
//                while ((rRen = ins.read(buffer)) != -1) {
//                    for (int i = 0; i < rRen; i++) {
//                        if (buffer[i] == 1) {
//                            readDowncount++;
//                        }
//                        System.out.println("ReadDownload: " + readDowncount + " " + buffer[i]);
//                    }
//                }

//                if (sendCount != readDowncount) {
//                    System.out.println("Expected: " + sendCount + " got: " + readDowncount);
//                    throw new Exception("Expected: " + sendCount + " got: " + readDowncount);
//                }

                // check size:
                if (remoteFile.getLength() != newLocalFile.getLength()) {
                    System.err.println("-----Expected: " + remoteFile.getLength() + " got: " + newLocalFile.getLength());
                    throw new Exception("Expected: " + remoteFile.getLength() + " got: " + newLocalFile.getLength());
                }



            } catch (VlException ex) {
                Logger.getLogger(TestCloudFS.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                newLocalFile.delete();
                remoteFile.delete();
            }
        }

    }

    private static void measureUpload() throws VRLSyntaxException, InterruptedException, ExecutionException, VlException {
        VDir dataset = VFSClient.getDefault().getDir("file:///" + System.getProperty("user.home") + "/Downloads/testData/dataset/");
        long start = System.currentTimeMillis();
        dataset.copyTo(getRemoteTestDir());
        long end = System.currentTimeMillis();
        System.out.println("Elapsed: " + (end - start));
    }

    private static Properties getCloudProperties()
            throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        String propPath = System.getProperty("user.home") + File.separator
                + "workspace" + File.separator + "nl.uva.vlet.vfs.cloud"
                + File.separator + "etc" + File.separator + "cloud.properties";
        File f = new File(propPath);
        properties.load(new FileInputStream(f));

        return properties;
    }

    private static void testGetInputStream() throws IOException {
        BasicHttpParams params = new BasicHttpParams();
        org.apache.http.params.HttpConnectionParams.setSoTimeout(params, 30000);
        params.setParameter("http.socket.timeout", 30000);



        HttpGet getMethod = new HttpGet("https://149.156.10.131:8443/auth/v1.0/");
        getMethod.getParams().setIntParameter("http.socket.timeout", 30000);
        getMethod.setHeader("x-auth-user", uname);
        getMethod.setHeader("x-auth-key", key);
        PoolingClientConnectionManager cm = new PoolingClientConnectionManager();

        DefaultHttpClient client = new DefaultHttpClient(cm, params);
        HttpClient wrapClient1 = wrapClient(client);

        HttpResponse resp = wrapClient1.execute(getMethod);
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new IOException(resp.getStatusLine().toString());
        }
        Header storageURLHeader = resp.getFirstHeader("X-Storage-Url");
        Header authToken = resp.getFirstHeader("X-Auth-Token");

        
        getMethod = new HttpGet(storageURLHeader.getValue()+"/a4958c90-e0bb-4b0e-be3b-517c6e2c1629-testLargeUpload");
        getMethod.getParams().setIntParameter("http.socket.timeout", 30000);
        getMethod.setHeader(authToken);
        getMethod.setHeader(authToken);
        resp = wrapClient1.execute(getMethod);
        Header[] headers = resp.getAllHeaders();
        for(int i=0;i<headers.length;i++){
            System.out.println(headers[i].getName() + " : "+headers[i].getValue());
        }
        
        EntityUtils.consume(resp.getEntity());
        
        wrapClient1.getConnectionManager().closeExpiredConnections();
    }

    private static org.apache.http.client.HttpClient wrapClient(org.apache.http.client.HttpClient base) {
        try {
            SSLSocketFactory ssf = getSSLSocketFactory();
            ClientConnectionManager ccm = base.getConnectionManager();
            SchemeRegistry sr = ccm.getSchemeRegistry();
            sr.register(new Scheme("https", ssf, 443));
            return new DefaultHttpClient(ccm, base.getParams());
        } catch (Exception ex) {
            return null;
        }
    }

    private static SSLSocketFactory getSSLSocketFactory() throws KeyManagementException, NoSuchAlgorithmException {
        SSLContext ctx = getSSLContext();

        SSLSocketFactory ssf = new SSLSocketFactory(ctx);
        ssf.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        return ssf;
    }

    private static SSLContext getSSLContext() throws KeyManagementException, NoSuchAlgorithmException {
        SSLContext ctx = SSLContext.getInstance("TLS");
        X509TrustManager tm = new X509TrustManager() {

            @Override
            public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        };
        ctx.init(null, new TrustManager[]{tm}, null);
        return ctx;
    }
}
