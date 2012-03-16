package nl.uva.vlet.vfs.test;

import java.net.MalformedURLException;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.Certificate;
import java.net.URL;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLPeerUnverifiedException;
import junit.framework.Assert;
import nl.uva.vlet.ClassLogger;
import nl.uva.vlet.exception.ResourceAlreadyExistsException;
import nl.uva.vlet.exception.ResourceCreationFailedException;
import nl.uva.vlet.exception.ResourceException;
import nl.uva.vlet.exception.VRLSyntaxException;
import nl.uva.vlet.exception.VlException;
import nl.uva.vlet.vfs.VDir;
import nl.uva.vlet.vfs.VFSClient;
import nl.uva.vlet.vfs.VFSNode;
import nl.uva.vlet.vfs.VFile;
import nl.uva.vlet.vfs.VFileSystem;
import nl.uva.vlet.vfs.cloud.CloudFSFactory;
import nl.uva.vlet.vfs.cloud.CloudFileSystem;
import nl.uva.vlet.vrl.VRL;
import nl.uva.vlet.vrs.ServerInfo;
import nl.uva.vlet.vrs.VRS;
import nl.uva.vlet.vrs.VRSContext;

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

            // testLoc = new VRL(
            // "swift://149.156.10.131:8443/auth/v1.0/TEST_VRS_LOC");


            testLoc = new VRL(
                    "filesystem:/testBlobStoreVFS");
        } catch (VRLSyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

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

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            VRS.exit();
        }

    }

    private static void testToch() throws InterruptedException,
            ExecutionException, VlException {
        VRL vrl = testLoc.append("/test_1/deleteMe/dir1/FILE2");

        boolean exists = cloudFS.openLocation(vrl).exists();

        if (exists) {
            cloudFS.rm(vrl);
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
            IOException, InterruptedException, ExecutionException {
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
            IOException, InterruptedException, ExecutionException {
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
            InterruptedException, ExecutionException {
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
        boolean success = cloudFS.rm(vrl);

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
            InterruptedException, ExecutionException {
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

        cloudFS.rm(vrl2);
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

        info.setUsername("vph_dev:admin");
        info.setPassword("secret");
        info.store();

        CloudFSFactory fsFactory = new CloudFSFactory();

        cloudFS = (CloudFileSystem) fsFactory.createNewFileSystem(context,
                info, testLoc);

        cloudFS.createDir(testLoc, true);
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

    private static void testCreateDirectoryWhileFileWithSameNameExists()
            throws VlException {
        String fdname = "testfiledir2";

        if (getRemoteTestDir().existsDir(fdname)) {
            // previous test went wrong !!
            logger.debugPrintf("*** Warning: Remote testfile already exists and is a directory");
            VDir dir = getRemoteTestDir().getDir(fdname);
            dir.delete(false);

            Assert.assertFalse(
                    "Could not remote previous test directory. Please remove it manually:"
                    + dir, dir.exists());
            // fail("Remote testfile is already directory. Please remove previous test directory!");
        }

        VFile newfile = getRemoteTestDir().createFile("testfiledir2");

        // MUST return false!
        Assert.assertFalse(
                "existsDir() must return FALSE when file with same name already exists!",
                getRemoteTestDir().existsDir("testfiledir2"));

        try {
            VDir newDir = getRemoteTestDir().createDir("testfiledir2");
            Assert.fail("Create directory out of existing file should raise Exception:");
        } // both are allowed:
        catch (ResourceCreationFailedException e) {
            // e.printStackTrace();
        } catch (ResourceAlreadyExistsException e) {
            // e.printStackTrace();
        }

        newfile.delete();
    }

    private static void testExceptionsExistingDir() throws VlException {
        VDir newDir = getRemoteTestDir().createDir("testExistingDir2");

        try {
            // create and do NOT ignore:
            newDir = getRemoteTestDir().createDir("testExistingDir2", false);
            newDir.delete();
            Assert.fail("createDir(): Should raise Exception:"
                    + ResourceAlreadyExistsException.class);
        } catch (ResourceAlreadyExistsException e) {
            // e.printStackTrace();
        }

        newDir.delete();
        VFile newFile = getRemoteTestDir().createFile("testExistingFile2");

        try {
            // create Dir and do NOT ignore existing File or Dir:
            newDir = getRemoteTestDir().createDir("testExistingFile2", false);
            newDir.delete();
            Assert.fail("createDir(): Should raise Exception:"
                    + ResourceAlreadyExistsException.class + " or "
                    + ResourceCreationFailedException.class);
        } // also allowed as the intended resource doesn't exists as exactly
        // the same type: existing Directory is not the intended File
        catch (ResourceCreationFailedException e) {
            // e.printStackTrace();
        } catch (ResourceAlreadyExistsException e) {
            // e.printStackTrace();
        } catch (ResourceException e) {
            // e.printStackTrace();

            Assert.fail("createDir(): Although a resource execption is better then any other,"
                    + "this unit test expects either:"
                    + ResourceAlreadyExistsException.class
                    + " or "
                    + ResourceCreationFailedException.class);
            // Global.debugPrintStacktrace(e);
        }
        newFile.delete();
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
}
