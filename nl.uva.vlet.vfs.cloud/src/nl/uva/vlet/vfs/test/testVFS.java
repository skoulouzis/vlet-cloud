/*
 * Copyright 2006-2011 The Virtual Laboratory for e-Science (VL-e) 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").  
 * You may not use this file except in compliance with the License. 
 * For details, see the LICENCE.txt file location in the root directory of this 
 * distribution or obtain the Apache Licence at the following location: 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * See: http://www.vl-e.nl/ 
 * See: LICENCE.txt (located in the root folder of this distribution). 
 * ---
 * $Id: testVFS.java,v 1.3 2012-02-27 14:33:22 skoulouz Exp $  
 * $Date: 2012-02-27 14:33:22 $
 */
// source: 
package nl.uva.vlet.vfs.test;

import java.io.IOException;
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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;
import nl.uva.vlet.Global;
import nl.uva.vlet.data.IntegerHolder;
import nl.uva.vlet.data.StringHolder;
import nl.uva.vlet.data.StringList;
import nl.uva.vlet.data.StringUtil;
import nl.uva.vlet.data.VAttribute;
import nl.uva.vlet.exception.ResourceAlreadyExistsException;
import nl.uva.vlet.exception.ResourceCreationFailedException;
import nl.uva.vlet.exception.ResourceException;
import nl.uva.vlet.exception.ResourceNotFoundException;
import nl.uva.vlet.exception.ResourceWriteAccessDeniedException;
import nl.uva.vlet.exception.VlException;
import nl.uva.vlet.io.StreamUtil;
import nl.uva.vlet.tasks.ActionTask;
import nl.uva.vlet.tasks.DefaultTaskMonitor;
import nl.uva.vlet.tasks.ITaskMonitor;
import nl.uva.vlet.util.ChecksumUtil;
import nl.uva.vlet.util.bdii.BdiiService;
import nl.uva.vlet.util.bdii.StorageArea;
import nl.uva.vlet.vdriver.vfs.localfs.LFile;
import nl.uva.vlet.vfs.VChecksum;
import nl.uva.vlet.vfs.VDir;
import nl.uva.vlet.vfs.VFSClient;
import nl.uva.vlet.vfs.VFSNode;
import nl.uva.vlet.vfs.VFile;
import nl.uva.vlet.vfs.VFileSystem;
import nl.uva.vlet.vfs.VLogicalFileAlias;
import nl.uva.vlet.vfs.VReplicatable;
import nl.uva.vlet.vfs.VThirdPartyTransferable;
import nl.uva.vlet.vfs.VUnixFileAttributes;
import nl.uva.vlet.vrl.VRL;
import nl.uva.vlet.vrs.ServerInfo;
import nl.uva.vlet.vrs.VCommentable;
import nl.uva.vlet.vrs.VRSContext;
import nl.uva.vlet.vrs.io.VRandomAccessable;
import nl.uva.vlet.vrs.io.VRandomReadable;
import nl.uva.vlet.vrs.io.VRandomReader;
import nl.uva.vlet.vrs.io.VResizable;
import nl.uva.vlet.vrs.io.VZeroSizable;

/**
 * This is an abstract test class which must be subclassed by a VFS
 * implementation.
 *
 *
 * @author P.T. de Boer
 */
public class testVFS extends VTestCase {

    private static final String TEST_CONTENTS = ">>> This is a testfile used for the VFS unit tests  <<<\n"
            + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\n"
            + "0123456789@#$%*()_+\n"
            + "Strange characters:...<TODO>\n"
            + "UTF8:<TODO>\n" + "\n --- You Can Delete this File ---\n";
    // ========================================================================
    // Instance
    // ========================================================================
    private VDir remoteTestDir = null;
    protected VDir localTempDir = null;
    private boolean testRenames = true;
    private boolean doWrites = true;
    private boolean doBigTests = true;
    private VRL localTempDirVrl = TestSettings.getTestLocation(TestSettings.VFS_LOCAL_TEMPDIR_LOCATION);
    private VRL remoteTestDirVrl = null;
    private int uniquepathnr = 0;
    private Object uniquepathnrMutex = new Object();

    /**
     * Return path with incremental number to make sure each new file did exist
     * before
     */
    public String nextFilename(String prefix) {
        synchronized (uniquepathnrMutex) {
            return prefix + uniquepathnr++;
        }
    }

    /**
     * Override this method if the local test dir has to have a different
     * location
     */
    public VRL getLocalTempDirVRL() {
        return localTempDirVrl;
    }

    public VRL getRemoteLocation() {
        return remoteTestDirVrl;
    }

    public void setRemoteLocation(VRL remoteLocation) {
        this.remoteTestDirVrl = remoteLocation;
    }
    private Object setupMutex = new Object();
    private VRL otherRemoteLocation = null;
    private boolean testEncodedPaths = true;
    private boolean testStrangeChars = true;

    /**
     * Sets up the tests fixture. (Called before every tests case method.)
     *
     * @throws VlException
     */
    protected void setUp() throws VlException {
        verbose(3, "setUp(): Checking remote test location:"
                + getRemoteLocation());
        VDir dir;
        VRL loc;

        synchronized (setupMutex) {
            // create/get only if VDir hasn't been fetched/created before !
            if (getRemoteTestDir() == null) {

                if (getVFS().existsDir(getRemoteLocation())) {
                    setRemoteTestDir(getVFS().getDir(getRemoteLocation()));

                    verbose(3, "setUp(): Using remoteDir:" + getRemoteTestDir());

                } else {
                    // create complete path !
                    try {
                        verbose(1, "creating new remote test location:"
                                + getRemoteLocation());
                        loc = getRemoteLocation();
                        dir = getVFS().mkdirs(loc);
                        setRemoteTestDir(dir);

                        verbose(1, "New created remote test directory="
                                + getRemoteTestDir());

                    } catch (VlException e) {
                        e.printStackTrace();
                        throw e;
                    }

                    verbose(1, "created new remote test location:"
                            + getRemoteTestDir());
                }

            }

            if (localTempDir == null) {
                VRL localdir = getLocalTempDirVRL();

                if (getVFS().existsDir(localdir)) {
                    localTempDir = getVFS().getDir(localdir);
                    // localTempDir.delete(true);
                } else {
                    // create complete path !
                    localTempDir = getVFS().mkdirs(localdir, true);

                    verbose(1, "created new local test location:"
                            + localTempDir);
                }
            }
        }
    }

    /**
     * Tears down the tests fixture. (Called after every tests case method, so
     * no real cleanup can be done here)
     */
    protected void tearDown() {
    }

    boolean getTestEncodedPaths() {
        return this.testEncodedPaths;
    }

    void setTestEncodedPaths(boolean doEncoding) {
        this.testEncodedPaths = doEncoding;
    }

    boolean getTestStrangeCharsInPaths() {
        return this.testStrangeChars;
    }

    boolean getTestRenames() {
        return testRenames;
    }

    // =======
    // =======
    /**
     * Print some info before starting
     *
     * @throws VlException
     */
    public void testPrintInfo() throws VlException {
        ServerInfo info = this.getVFS().getVRSContext().getServerInfoFor(this.getRemoteLocation(), false);

        message(" --- Test Info ---");
        message(" remote test dir         =" + getRemoteTestDir());
        message(" remote test dir exists  =" + getRemoteTestDir().exists());
        message(" - do big tests          =" + this.getTestDoBigTests());
        message(" - do write tests        =" + this.getTestWriteTests());
        message(" - do rename tests       =" + this.getTestRenames());
        message(" - do strange char tests ="
                + this.getTestStrangeCharsInPaths());
        message(" - do URL en-decoding    =" + this.getTestEncodedPaths());
        message("--- info ---");
        message("" + info);

        if (info != null) {
            message("--- ServerInfo ---");
            message(" - Host:port           =" + info.getHostname() + ":"
                    + info.getPort());
            message(" - remote home         =" + info.getDefaultPath());
            message(" - info.getUsePassive()=" + info.getUsePassiveMode(false));
        }

    }

    public void testAAAFirst() throws VlException {
        // testListDirIterator();
        // testRenameWithSpaces();
//        testZCreateDirectoryWhileFileWithSameNameExists();
    }

    /**
     * Exist is also a basic method used a lot in the unit tests and VRS
     * methods. <p>
     *
     * The exists() methods should return true if resource exists, false if it
     * doesn't exists and only throw an exception when the method couldn't
     * determine whether the resource exists. Other methods like getFile() MUST
     * return an ResourceNotFound exception when the resource doesn't exist.
     */
    public void testExists() throws VlException {
        boolean result = getRemoteTestDir().existsFile(
                "ThisFileShouldnotexist_1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ");
        Assert.assertFalse("Exists(): file should not exist!", result);

        result = getRemoteTestDir().existsDir(
                "ThisDirShouldnotexist_1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ");
        Assert.assertFalse("Exists(): Dir should not exist!", result);

        result = getRemoteTestDir().exists();
        Assert.assertTrue(
                "Exists(): *** ERROR: Remote Test directory doesn't exists. Tests will fail",
                result);

        if (this.getTestEncodedPaths()) {
            result = getRemoteTestDir().existsFile(
                    "This File Should not exist 1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ");
            Assert.assertFalse("Exists(): file should not exist!", result);

            result = getRemoteTestDir().existsDir(
                    "This Dir Should not exist 1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ");
            Assert.assertFalse("Exists(): Dir should not exist!", result);
        }
    }

    public void testRootExists() throws VlException {
        VRL rootPath = null;
        ServerInfo inf = this.getServerInfo();
        // Use "/" or explicit rootPath
        if (inf != null) {
            rootPath = inf.getRootPath();
        } else {
            rootPath = this.getRemoteLocation().copyWithNewPath("/");
        }

        boolean result = getRemoteTestDir().existsDir(rootPath.getPath());
        Assert.assertTrue("Exists(): root path  '" + rootPath
                + "' Doesn't exist!", result);
    }

    public ServerInfo getServerInfo() throws VlException {
        return this.getVFS().getVRSContext().getServerInfoFor(this.remoteTestDir.getVRL(), false);
    }

    /**
     * Regression bug 199: Test whether "/~" resolves to remote (Default) home
     * location and 'exists()'.
     */
    public void testTildeExpansion() throws VlException {
        VFSNode node = getRemoteTestDir().getPath("/~");
        Assert.assertTrue(
                "Default HOME reports is does not exist (SRB might do this):'/~' => '"
                + node + "'", node.exists());
    }

    /**
     * Should be first test, since other tests methods create and delete files
     * for testing.
     *
     * @throws VlException
     */
    public void testCreateAndDeleteFile() throws VlException {
        verbose(1, "Remote testdir=" + getRemoteTestDir());

        // ---
        // Use createFile() method
        // ---

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

    /**
     * Test FileSystem.createFile(), should behave the same as VDir.createFile()
     *
     * @throws VlException
     */
    public void testFSCreateAndDeleteFile() throws VlException {
        verbose(1, "Remote testdir=" + getRemoteTestDir());

        VRL fullpath = getRemoteTestDir().resolvePathVRL(
                nextFilename("testFSFile"));
        VFileSystem fs = getRemoteTestDir().getFileSystem();

        // ===
        // use createFile:
        // ===

        VFile newFile = createFile(fs, fullpath, true);

        // sftp created 1-length new files !
        Assert.assertNotNull("New created file may not be NULL", newFile);
        Assert.assertTrue("Length of newly created file must be 0!:"
                + getRemoteTestDir(), newFile.getLength() == 0);

        newFile.delete();
        Assert.assertFalse(
                "After deletion, a file may NOT report it still 'exists'!",
                newFile.exists());

        // ===
        // use FileSystem.newFile().create()
        // ===
        fullpath = getRemoteTestDir().resolvePathVRL(nextFilename("testFSFile"));
        newFile = fs.newFile(fullpath);
        newFile.create();

        // sftp created 1-length new files !
        Assert.assertNotNull("New created file may not be NULL", newFile);
        Assert.assertTrue("Length of newly created file must be 0!:"
                + getRemoteTestDir(), newFile.getLength() == 0);

        newFile.delete();
        Assert.assertFalse(
                "After deletion, a file may NOT report it still 'exists'!",
                newFile.exists());
    }

    private VFile createFile(VFileSystem fs, VRL fullpath, boolean ignore)
            throws VlException {
        VFile file = fs.newFile(fullpath);
        file.create(ignore);
        return file;
    }

    /**
     * Should be first test, since other tests methods create and delete files
     * for testing.
     *
     * @throws VlException
     */
    public void testCreateAndDeleteFullpathFile() throws VlException {
        verbose(1, "Remote testdir=" + getRemoteTestDir());

        VRL filevrl = getRemoteTestDir().getVRL().append(
                nextFilename("testFileB"));

        // use fullpath:
        VFile newFile = getRemoteTestDir().createFile(filevrl.getPath());

        // sftp created 1-length new files !
        Assert.assertNotNull(
                "New created file may not be NULL. Must throw exception",
                newFile);
        Assert.assertTrue("Length of new created file must be of 0 size:"
                + getRemoteTestDir(), newFile.getLength() == 0);
        Assert.assertEquals(
                "File should use complete pathname as new file name",
                filevrl.getPath(), newFile.getPath());

        // cleanup:
        boolean result = newFile.delete();
        Assert.assertTrue(
                "Cleanup after unit test failed. Deletion failed for:"
                + newFile, result);
        Assert.assertFalse(
                "After deletion, a file may NOT report it still 'exists'!",
                newFile.exists());

        // Test ./File !
        filevrl = getRemoteTestDir().resolvePathVRL(
                "./" + nextFilename("testFileC"));

        // use fullpath:
        newFile = getRemoteTestDir().createFile(filevrl.getPath());

        // sftp created 1-length new files !
        Assert.assertNotNull(
                "New created file may not be NULL. Must throw exception",
                newFile);
        Assert.assertTrue("Length of new created file must be of 0 size:"
                + getRemoteTestDir(), newFile.getLength() == 0);
        Assert.assertEquals(
                "File should use complete pathname as new file name",
                filevrl.getPath(), newFile.getPath());

        // cleanup:

        result = newFile.delete();
        Assert.assertTrue(
                "Cleanup after unit test failed. Deletion failed for:"
                + newFile, result);
        Assert.assertFalse(
                "After deletion, a file may NOT report it still 'exists'!",
                newFile.exists());

    }

    /**
     * Should be fist tests, since other tests methods create and delete file
     * their own tests files
     *
     * @throws VlException
     */
    public void testCreateAndDeleteFileWithSpace() throws VlException {
        if (this.getTestEncodedPaths() == false) {
            message("***Warning: Skipping test:testCreateDeleteFileWithSpaceAndListParentDir");
            return;
        }

        VFile newFile = getRemoteTestDir().createFile(
                nextFilename("test File D"));
        newFile.delete();

    }

    /**
     * Regression test for Grid FTP: remote directory list does NOT like spaces
     * !
     *
     * @throws VlException
     */
    public void testCreateDeleteFileWithSpaceAndListParentDir()
            throws VlException {
        if (this.getTestEncodedPaths() == false) {
            message("***Warning: Skipping test:testCreateDeleteFileWithSpaceAndListParentDir");
            return;
        }

        VFile newFile = getRemoteTestDir().createFile(
                nextFilename("test File E"));

        verbose(1, "remote filename with space=" + newFile);

        VFSNode[] names = getRemoteTestDir().list();

        Assert.assertNotNull(
                "Remote directory contents is NULL after creation of file in:"
                + getRemoteTestDir(), names);

        Assert.assertFalse(
                "Remote directory contents is empty after creation of file in:"
                + getRemoteTestDir(), names.length <= 0);

        newFile.delete();
    }

    public void testCreateAndIgnoreExistingFile() throws VlException {
        VFile newFile = getRemoteTestDir().createFile(nextFilename("testFileF"));

        // current default implemenation is to ignore existing files!

        newFile = getRemoteTestDir().createFile(nextFilename("testFileX"));

        newFile = getRemoteTestDir().createFile(nextFilename("testFileY"), true);

        newFile.delete();
    }

    public void testFileAttributes() throws VlException {
        VFile newFile = getRemoteTestDir().newFile(nextFilename("testFileAttr"));

        testVFSNodeAttributes(newFile);

        newFile.create();
        testVFSNodeAttributes(newFile);

        newFile.delete();
    }

    // =======================================================================
    // Directory Creation Tests
    // =======================================================================
    /**
     * Test Attribute interface with matching methods !
     */
    private void testVFSNodeAttributes(VFSNode newFile) throws VlException {

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

    /**
     * Other tests realy heavily on the creation and deletion of (remote)
     * directories!
     *
     * @throws VlException
     */
    public void testCreateAndDeleteSubDir() throws VlException {
        VDir newDir = getRemoteTestDir().createDir(nextFilename("testDirA"));

        newDir.delete();

        Assert.assertFalse("Directory should not exist after deletion",
                newDir.exists());
    }

    /**
     * Regression for SRM: cannot delete directory which contains directory and
     * a file.
     */
    public void testCreateAndDeleteRecursiveDir() throws VlException {

        VDir newDir = getRemoteTestDir().createDir(nextFilename("testDirB"));
        // create subdir:
        VDir subDir = newDir.createDir("subDir1");
        subDir = newDir.createDir("subDir2");
        subDir = newDir.createDir("subDir3");
        newDir.createFile("subFile1");
        newDir.createFile("subFile2");

        VDir subsubDir = subDir.createDir("subsubDir1");
        subsubDir.createFile("subsubsubfile");

        subDir.createDir("subsubDir2");
        subDir.createFile("subsubFile1");
        subDir.createFile("subsubFile2");

        newDir.delete(true);

        Assert.assertFalse("Directory should not exist after deletion",
                newDir.exists());
    }

    /**
     * Test whether a directory can take a full (absolute) path as directory
     * name. New since 0.9.2 !
     *
     * @throws VlException
     */
    public void testCreateAndDeleteFullDir() throws VlException {
        VRL fullPath = getRemoteTestDir().resolvePathVRL(
                nextFilename("testDirC"));

        // get parent dir to check whether directory allow (sub) directories in
        // the create method.
        VDir parentDir = getRemoteTestDir().getParent();
        VDir newDir = parentDir.createDir(fullPath.getPath());
        Assert.assertEquals(
                "Directory should use complete pathname as new directory name.",
                fullPath.getPath(), newDir.getPath());
        newDir.delete();

        // check ./path !
        fullPath = getRemoteTestDir().resolvePathVRL(
                "./" + nextFilename("testDirD"));
        newDir = parentDir.createDir(fullPath.getPath());
        Assert.assertEquals(
                "Directory should use complete pathname as new directory name.",
                fullPath.getPath(), newDir.getPath());
        // delete parent !
        newDir.delete(true);

        // New Since 0.9.2:
        // create full directory paths in between ! (as default)
        //
        String pathStr = getRemoteTestDir().getPath() + VRL.SEP_CHAR
                + "testDir3d";
        newDir = getRemoteTestDir().createDir(pathStr);
        Assert.assertEquals(
                "Directory should use complete pathname as new directory name",
                pathStr, newDir.getPath());
        // delete parent of parent !
        newDir.delete(true);

    }

    /**
     * Test VFileSystem.createDir() should behave the same as VDir.createDir()
     * ... New since 0.9.2 !
     *
     * @throws VlException
     */
    public void testFSCreateAndDeleteDir() throws VlException {
        VRL fullPath = getRemoteTestDir().resolvePathVRL(
                nextFilename("testFSDirE"));
        VFileSystem fs = getRemoteTestDir().getFileSystem();

        VDir newDir = createDir(fs, fullPath, true);
        Assert.assertEquals(
                "Directory should use complete pathname as new directory name",
                fullPath.getPath(), newDir.getPath());
        newDir.delete();

    }

    private VDir createDir(VFileSystem fs, VRL dirpath, boolean ignore)
            throws VlException {
        VDir dir = fs.newDir(dirpath);
        dir.create(ignore);
        return dir;
    }

    // not all implementations can handle spaces
    public void testCreateAndDeleteDirWithSpace() throws VlException {
        if (this.getTestEncodedPaths() == false) {
            message("***Warning: Skipping test:testCreateAndDeleteDirWithSpace");
            return;
        }

        VDir newDir = getRemoteTestDir().createDir("test Dir F");

        newDir.delete();
    }

    public void testCreateAndIgnoreExistingDir() throws VlException {
        String dirname = "testDirG";

        VDir newDir = getRemoteTestDir().createDir(dirname);

        // current default implemenation is to ignore existing directories!

        newDir = getRemoteTestDir().createDir(dirname);

        try {
            newDir = getRemoteTestDir().createDir(dirname, true);
        } catch (Exception e) {
            fail("Caught Exception: When setting ignoreExisting==true. Method createDir() must ignore the already existing directory");
        }

        newDir.delete();
    }

    public void testCreateAndRenameDir() throws VlException {
        // must create a d
//        String newDirName = "renamedTestDirG";
//
//        VDir newDir = getRemoteTestDir().createDir("testDirH");
//
//        if (getTestRenames() == true) {
//            boolean result = newDir.renameTo(newDirName, false);
//
//            Assert.assertEquals("Rename must return true", true, result);
//
//            result = getRemoteTestDir().existsDir(newDirName);
//            Assert.assertEquals("New VDir doesn't exist:" + newDirName, true,
//                    result);
//
//            VDir renamedDir = getRemoteTestDir().getDir(newDirName);
//            Assert.assertNotNull(
//                    "After rename, new VDir is NULL:" + newDirName, renamedDir);
//
//            // cleanup:
//            renamedDir.delete();
//        } else {
//            newDir.delete();
//        }
    }

    public void testCreateAndRenameFile() throws VlException {
//        if (getTestRenames() == false) {
//            message("Skipping rename test");
//            return;
//        }
//
//        String newFileName = "newFileName6";
//
//        VFile newFile = getRemoteTestDir().createFile(nextFilename("testFileH"));
//
//        {
//            boolean result = newFile.renameTo(newFileName, false);
//
//            Assert.assertEquals("Rename should return true", true, result);
//
//            result = getRemoteTestDir().existsFile(newFileName);
//            Assert.assertEquals("new File should exist:" + newFileName, true,
//                    result);
//
//            VFile renamedFile = getRemoteTestDir().getFile(newFileName);
//            Assert.assertNotNull("new VDir is NULL", renamedFile);
//
//            // cleanup:
//            renamedFile.delete();
//        }
    }

    public void testRenameWithSpaces() throws VlException {
//        if (getTestRenames() == false) {
//            message("Skipping rename test");
//            return;
//        }
//
//        if (getTestStrangeCharsInPaths() == false) {
//            message("Skipping rename with spaces test");
//            return;
//        }
//
//        VDir parentDir = getRemoteTestDir();
//
//        // postfix space
//
//        String orgFileName = nextFilename("spaceRenameFile1");
//        String newFileName = orgFileName + " ";
//
//        testRename(parentDir, orgFileName, newFileName);
//
//        // prefix space
//        orgFileName = nextFilename("spaceRenameFile2");
//        newFileName = " " + orgFileName + " ";
//
//        // infix space
//        testRename(parentDir, orgFileName, newFileName);
//        orgFileName = "infixSpaceRenameTest000";
//        newFileName = "infixSpace RenameTest000";
//
//        testRename(parentDir, orgFileName, newFileName);
    }

    private void testRename(VDir parentDir, String orgFileName,
            String newFileName) throws VlException {
//        VFile orgFile = parentDir.createFile(orgFileName);
//
//        message("Rename: '" + orgFileName + "' to '" + newFileName + "'");
//        message("Rename:  - orgfile='" + orgFile + "'");
//
//        boolean result = getRemoteTestDir().existsFile(newFileName);
//        Assert.assertFalse(
//                "Remote file system claims new file already exists!:'"
//                + newFileName + "'", result);
//
//        result = orgFile.renameTo(newFileName, false);
//        Assert.assertEquals("Rename should return true", true, result);
//
//        result = getRemoteTestDir().existsFile(newFileName);
//        Assert.assertTrue("new File should exist:" + newFileName, result);
//
//        VFile renamedFile = getRemoteTestDir().getFile(newFileName);
//        message("Rename:  - renamefile='" + renamedFile + "'");
//
//        Assert.assertNotNull("getFile() of renamed file returned NULL",
//                renamedFile);
//        Assert.assertTrue("New file must exist after rename!",
//                renamedFile.exists());
//
//        // rename back
//        result = renamedFile.renameTo(orgFileName, false);
//        Assert.assertEquals("Rename should return true", true, result);
//        VFile rerenamedFile = getRemoteTestDir().getFile(orgFileName);
//        Assert.assertNotNull("getFile() of reerenamed file return NULL",
//                rerenamedFile);
//        Assert.assertTrue("Original file must exist after double rename!",
//                rerenamedFile.exists());
//
//        rerenamedFile.delete();
    }

    /**
     * Create empty dir, copy it and delete both. Regression test for SRM
     *
     * @throws VlException
     */
    public void testCopyEmptyDir() throws VlException {
        VRL sourcePath = getRemoteTestDir().resolvePathVRL(
                nextFilename("testFSDirEmpty_original"));
        VFileSystem fs = getRemoteTestDir().getFileSystem();

        VDir destParentDir = getRemoteTestDir();

        VDir sourceDir = createDir(fs, sourcePath, true);
        Assert.assertEquals(
                "Directory should use complete pathname as new directory name",
                sourcePath.getPath(), sourceDir.getPath());

        VDir copyDir = sourceDir.copyTo(destParentDir, "copyof_emptyTestDir");

        sourceDir.delete();
        copyDir.delete();

    }

    // =======================================================================
    // Robustness tests
    // =======================================================================

    /*
     * Since the Copy/Move methods highly depend on the create/exists/and delete
     * methods, do extra robustness tests to check correct behaviour in
     * incorrect situations.
     *
     * Scenarios which caused lot of trouble with Gftp and SRB:
     *
     * - Create file when file already exists (and ignoreExisting==false) -
     * Create dir when dir already exists (and ignoreExisting==false) - Create
     * file when directory with same name already exists - Create directory when
     * file with same name already exists
     */
    public void testCreateDirPermissionDenied() throws VlException {
        try {
            VDir root = getRemoteTestDir().getRoot();
            // "/test" is illegal under linux
            if (Global.isLinux() == true) {
                if (root.isWritable() == false) {
                    VDir newDir = root.createDir(nextFilename("testI"));

                    fail("Should raise Exception:"
                            + ResourceWriteAccessDeniedException.class);
                }
                // ; continue
            }
            if (Global.isWindows() == true) {
                // Need read only dir. Do these exists under windows ?
            }

        } // Compromize: not all implementation can check wether it was an access
        // denied
        catch (Exception e) // catch (WriteAccessDeniedException e)
        {
            Global.debugPrintln(this, "Caught expected Exception:" + e);
            Global.debugPrintStacktrace(e);
        }
    }

    // =======================================================================
    // VDir.list() filter tests
    // =======================================================================
    public void testListDirFiltered() throws VlException {
        VDir ldir = getRemoteTestDir().createDir("dirListTest");

        // list EMPTY dir:
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

            message("nr of filtered files '*' =" + result.length);

            result = ldir.list("*.txt", false);
            Assert.assertNotNull("List result may not be null", result);
            Assert.assertEquals("Number of returned files is not correct.", 2,
                    result.length);

            message("nr of filtered files '*.txt' =" + result.length);

            // test RE version of *.txt
            result = ldir.list(".*\\.txt", true);
            Assert.assertNotNull("List result may not be null", result);
            Assert.assertEquals("Number of returned files is not correct.", 2,
                    result.length);

            message("nr of filtered files '.*\\.txt' =" + result.length);

        } finally {
            ldir.delete(true);
        }

    }

    public void testListDirIterator() throws VlException {
        VDir ldir = getRemoteTestDir().createDir("dirListTest2");
        String fileName0 = "file0";
        String fileName1 = "file1";
        String fileName2 = "file2";

        try {
            ldir.createFile(fileName0);
            ldir.createFile(fileName1);
            ldir.createFile(fileName2);
            ldir.createFile("file3");

            IntegerHolder totalNumNodes = new IntegerHolder();

            // ===
            // Test range combinations.
            // ===

            // complete range:

            VFSNode[] totalResult = ldir.list(0, -1, totalNumNodes);
            Assert.assertNotNull("List result may not be null.", totalResult);
            Assert.assertEquals("Number of returned files is not correct.", 4,
                    totalResult.length);
            Assert.assertTrue("IntegerHolder may not contain NULL value.",
                    totalNumNodes.isSet());

            if (totalNumNodes.intValue() < 0) {
                warning("testListDirIterator(): totalNumNodes not supported!");
            } else {
                Assert.assertEquals("Total Number of nodes is not correct.", 4,
                        totalNumNodes.intValue());
            }
            // get first file:

            VFSNode[] result = ldir.list(0, 1, totalNumNodes);

            Assert.assertNotNull("List result may not be null.", result);
            Assert.assertEquals("Number of returned files is not correct.", 1,
                    result.length);
            Assert.assertTrue("IntegerHolder may not contain NULL value.",
                    totalNumNodes.isSet());
            if (totalNumNodes.intValue() < 0) {
                warning("testListDirIterator(): totalNumNodes not supported!");
            } else {
                Assert.assertEquals("Total Number of nodes is not correct.", 4,
                        totalNumNodes.intValue());
            }
            // Check with filename with entry in complete list
            Assert.assertEquals("Returned result is not correct.",
                    result[0].getName(), totalResult[0].getName());
            if (totalNumNodes.intValue() < 0) {
                warning("testListDirIterator(): totalNumNodes not supported!");
            } else {
                Assert.assertEquals("Total Number of nodes is not correct.", 4,
                        totalNumNodes.intValue());
            }
            message("test ListIterator Single result is =" + result[0]);

            result = ldir.list(1, 2, null);
            Assert.assertNotNull("List result may not be null.", result);
            Assert.assertEquals("Number of returned files is not correct.", 2,
                    result.length);
            Assert.assertTrue("IntegerHolder may not contain NULL value.",
                    totalNumNodes.isSet());
            // check entry against totalResult
            Assert.assertEquals("Returned result is not correct.",
                    result[0].getName(), totalResult[1].getName());
            Assert.assertEquals("Returned result is not correct.",
                    result[1].getName(), totalResult[2].getName());

            message("test ListIterator Single result is =" + result[0]);

            result = ldir.list(2, 1, totalNumNodes);

            Assert.assertNotNull("List result may not be null.", result);
            Assert.assertEquals("Number of returned files is not correct.", 1,
                    result.length);
            Assert.assertTrue("IntegerHolder may not contain NULL value.",
                    totalNumNodes.isSet());

            if (totalNumNodes.intValue() < 0) {
                warning("testListDirIterator(): totalNumNodes not supported!");
            } else {
                Assert.assertEquals("Total Number of nodes is not correct.", 4,
                        totalNumNodes.intValue());
            }

            // Check with filename with entry #2 in complete list
            Assert.assertEquals("Returned result is not correct.",
                    result[0].getName(), totalResult[2].getName());

            message("test ListIterator Single result is =" + result[0]);
        } finally {
            ldir.delete(true);
        }
    }

    // =======================================================================
    // Other
    // =======================================================================
    public void testACLs() throws VlException {
        VDir dir = this.getRemoteTestDir();

        // VFSNode read returns readonly ACL, so is never NULL, unless
        // an implementation screwed up.

        VAttribute[][] acl = dir.getACL();
        Assert.assertNotNull("ACLs for directoires not supported: ACL is NULL",
                acl);

        VFile file = dir.createFile("aclTestFile", true);

        acl = file.getACL();
        Assert.assertNotNull("ACLs not for files supported: ACL is NULL", acl);

        // entities are default NULL, so stop testing when no entities are
        // present
        VAttribute[] ents = dir.getACLEntities();

        // no entities: no support for ACLs: allowed
        if (ents == null) {
            file.delete();
            return;
        }

        Assert.assertFalse("non NULL ACL entity list may not be empty.",
                ents.length == 0);

        // get 1st entity
        VAttribute entity = ents[0];

        VAttribute[] record = file.createACLRecord(entity, true);
        Assert.assertNotNull("new entity returned NULL", record);

        file.delete();
    }

    // =======================================================================
    // Logical File Alias and Links:
    // =======================================================================
    public void testAlias() throws VlException {
        VFile orgFile = getRemoteTestDir().createFile("testLinkFileOriginal-1",
                true);
        String link1name = "testLinkto-1";
        String link2name = "testLinkto-2";
        VRL link1 = getRemoteTestDir().resolvePathVRL(link1name);
        VRL link2 = getRemoteTestDir().resolvePathVRL(link2name);

        if (getRemoteTestDir().existsFile(link1name)) {
            message("*** Warning: test link already exists. previous junit test failed. Removing:"
                    + link1);
            getRemoteTestDir().deleteFile(link1name);
        }

        if (getRemoteTestDir().existsFile(link2name)) {
            message("*** Warning: test link already exists. previous junit test failed. Removing:"
                    + link2);
            getRemoteTestDir().deleteFile(link2name);
        }

        try {
            if (orgFile instanceof VLogicalFileAlias) {
                VLogicalFileAlias lfn = (VLogicalFileAlias) orgFile;

                link1 = lfn.addAlias(link1);
                link2 = lfn.addAlias(link2);

                VRL vrls[] = lfn.getLinksTo();
                boolean hasLink1 = false;
                boolean hasLink2 = false;

                for (VRL vrl : vrls) {
                    if (vrl.equals(link1)) {
                        message("Original file has new link(1):" + link1);
                        hasLink1 = true;
                    }
                    if (vrl.equals(link2)) {
                        message("Original file has new link(2):" + link2);
                        hasLink2 = true;
                    }
                }

                Assert.assertTrue(
                        "New added link(1) not returned by getLinksTo():"
                        + link1, hasLink1);
                Assert.assertTrue(
                        "New added link(2) not returned by getLinksTo():"
                        + link2, hasLink2);

                VFile linkFile1 = getRemoteTestDir().getFile(link1name);
                VFile linkFile2 = getRemoteTestDir().getFile(link1name);

                Assert.assertTrue("Created link must exist:" + linkFile1,
                        linkFile1.exists());
                Assert.assertTrue("Created link must exist:" + linkFile2,
                        linkFile2.exists());

                if (linkFile1 instanceof VLogicalFileAlias) {
                    Assert.assertTrue(
                            "Created link must report it is an Alias:"
                            + linkFile1,
                            ((VLogicalFileAlias) linkFile1).isAlias());
                } else {
                    Assert.fail("Created linkfile (or alias) doesn' seem to support the VLogicalFileAlias!:"
                            + linkFile1);
                }

                if (linkFile2 instanceof VLogicalFileAlias) {
                    Assert.assertTrue(
                            "Created link must report it is an Alias:"
                            + linkFile2,
                            ((VLogicalFileAlias) linkFile2).isAlias());
                } else {
                    Assert.fail("Created linkfile (or alias) doesn' seem to support the VLogicalFileAlias!:"
                            + linkFile2);
                }
            } else {
                message("skipping VLogicalFileAlias tests for:" + orgFile);
            }
        } finally {
            message("Deleting:" + orgFile);
            orgFile.delete();

            message("Deleting:" + link1);
            if (getRemoteTestDir().existsFile(link1name)) {
                getRemoteTestDir().deleteFile(link1name);
            }

            message("Deleting:" + link2);
            if (getRemoteTestDir().existsFile(link2name)) {
                getRemoteTestDir().deleteFile(link2name);
            }
        }
    }

    // =======================================================================
    // Contents/ReadWrite
    // =======================================================================
    public void testSetGetSimpleContentsNewFile() throws VlException {
        if (getTestWriteTests() == false) {
            return;
        }

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
        //
        // test2: big string
        //
        if (getTestDoBigTests() == false) {
            return;
        }

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

            fail("Contents of file (big string) not the same, but small string does!.\n"
                    + "info=" + infoStr);
        }

        newFile.delete();
    }

    public void testSetGetSimpleContentsExistingFile() throws VlException {
        if (getTestWriteTests() == false) {
            return;
        }

        // create existing file first:
        VFile newFile = getRemoteTestDir().createFile("testFile7b");
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
        newFile = getRemoteTestDir().createFile("testFile7c");
        //
        // test2: big string
        //
        if (getTestDoBigTests() == false) {
            return;
        }

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

            fail("Contents of file (big string) not the same, but small string does!.\n"
                    + "info=" + infoStr);
        }

        newFile.delete();
    }

    public void testCopyMoveToRemote(boolean isMove) throws VlException {

        VFile localFile = null;
        VFile remoteFile = null;

        localFile = localTempDir.createFile("testLocalFile");
        localFile.setContents(TEST_CONTENTS);

        if (isMove) {
            remoteFile = localFile.moveTo(getRemoteTestDir());
        } else {
            remoteFile = localFile.copyTo(getRemoteTestDir());
        }

        Assert.assertNotNull("new remote File is NULL", remoteFile);

        String str = remoteFile.getContentsAsString();
        Assert.assertEquals("Contents of remote file not the same:" + str, str,
                TEST_CONTENTS);

        // file should be moved: local file may not exist.
        if (isMove == true) {
            Assert.assertEquals("local file should not exist:" + localFile,
                    false, localFile.exists());
        }

        if (isMove == false) {
            localFile.delete();
        }

        remoteFile.delete();

    }

    public void testMove10MBForthAndBack() throws Exception {
        if (getTestDoBigTests() == false) {
            return;
        }

        VFile localFile = null;
        VFile remoteFile = null;

        {
            localFile = localTempDir.createFile("test10MBmove");

            int len = 10 * 1024 * 1024;

            // create random file: fixed seed for reproducable tests
            Random generator = new Random(13);
            byte buffer[] = new byte[len];
            generator.nextBytes(buffer);
            verbose(1, "streamWriting to localfile:" + localFile);
            localFile.streamWrite(buffer, 0, buffer.length);

            // check contents:
            byte newcontents[] = localFile.getContents();
            int newlen = newcontents.length;
            // check size:
            Assert.assertEquals("size of new contents does not match.", len,
                    newlen);

            // move to remote (and do same basic asserts).
            long start_time = System.currentTimeMillis();
            verbose(1, "moving localfile to:" + getRemoteTestDir());
            remoteFile = localFile.moveTo(getRemoteTestDir());
            long total_millis = System.currentTimeMillis() - start_time;
            double up_speed = (len / 1024.0) / (total_millis / 1000.0);
            verbose(1, "upload speed=" + ((int) (up_speed * 1000)) / 1000.0
                    + "KB/s");

            verbose(1, "new remote file=" + remoteFile);

            Assert.assertNotNull("new remote File is NULL", remoteFile);
            Assert.assertTrue(
                    "after move to remote testdir, remote file doesn't exist:"
                    + remoteFile, remoteFile.exists());
            Assert.assertFalse(
                    "local file reports it still exists, after it has moved",
                    localFile.exists());

            // move back to local with new name (and do same basic asserts).
            start_time = System.currentTimeMillis();

            VFile newLocalFile = remoteFile.moveTo(this.localTempDir,
                    "test10MBback");
            Assert.assertNotNull("new local File is NULL", newLocalFile);
            Assert.assertFalse(
                    "remote file reports it still exists, after it has moved",
                    remoteFile.exists());
            total_millis = System.currentTimeMillis() - start_time;

            double down_speed = (len / 1024.0) / (total_millis / 1000.0);
            verbose(1, "download speed=" + ((int) (down_speed * 1000)) / 1000.0
                    + "KB/s");

            // check contents:
            newcontents = newLocalFile.getContents();
            newlen = newcontents.length;
            // check size:
            Assert.assertEquals("size of new contents does not match.", len,
                    newlen);

            // compare contents
            for (int i = 0; i < len; i++) {
                if (buffer[i] != newcontents[i]) {
                    Assert.assertEquals(
                            "Contents of file not the same. Byte nr=" + i,
                            buffer[i], newcontents[i]);
                }
            }

            newLocalFile.delete();
        }

    }

    /**
     * Create local file, move it to remote and streamRead it.
     */
    public void testStreamRead() throws Exception {
        if (getTestDoBigTests() == false) {
            return;
        }

        VFile localFile = null;
        VFile remoteFile = null;

        {
            localFile = localTempDir.createFile("test10MBStreamRead");

            int len = 10 * 1024 * 1024;

            // fixed seed for reproducable tests
            Random generator = new Random(13);

            byte buffer[] = new byte[len];
            generator.nextBytes(buffer);
            verbose(1, "Creating local file");
            localFile.streamWrite(buffer, 0, buffer.length);

            // move to remote (and do same basic asserts).
            verbose(1, "Moving file to:" + getRemoteTestDir());
            remoteFile = localFile.moveTo(getRemoteTestDir());
            Assert.assertNotNull("new remote File is NULL", remoteFile);
            Assert.assertFalse(
                    "local file reports it still exists, after it has moved",
                    localFile.exists());

            verbose(1, "Allocing new buffer, len=" + len);
            byte newcontents[] = new byte[len];

            verbose(1, "Getting inputstream of:" + remoteFile);
            InputStream inps = remoteFile.getInputStream();

            int nrread = 0;
            int totalread = 0;
            int prevp = 0;

            long read_start_time = System.currentTimeMillis();
            verbose(1, "Starting read loop");
            // read loop:
            while (totalread < len) {
                nrread = inps.read(newcontents, totalread, len - totalread);
                totalread += nrread;
                if (nrread < -1) {
                    verbose(1, "Error nread<0)");
                    break;
                }

                int perc = 100 * totalread / (len + 1);
                if ((perc / 10) > prevp) {
                    verbose(1, "nread=" + perc + "%");
                    prevp = perc / 10;
                }
            }

            long total_read_millis = System.currentTimeMillis()
                    - read_start_time;
            double speed = (len / 1024.0) / (total_read_millis / 1000.0);

            verbose(1, "read speed=" + ((int) (speed * 1000)) / 1000.0 + "KB/s");

            // check size:
            Assert.assertEquals(
                    "number of read bytes does not match file length", len,
                    totalread);

            // compare contents
            for (int i = 0; i < len; i++) {
                if (buffer[i] != newcontents[i]) {
                    Assert.assertEquals(
                            "Contents of file not the same. Byte nr=" + i,
                            buffer[i], newcontents[i]);
                }
            }

            remoteFile.delete();
        }

    }

    /**
     * Create remote file, write to it, and move it back to here to check
     * contents.
     */
    public void testStreamWrite() throws Exception {
        if (getTestDoBigTests() == false) {
            return;
        }

        VFile localFile = null;
        VFile remoteFile = null;

        {
            remoteFile = getRemoteTestDir().createFile("test10MBstreamWrite");

            int len = 10 * 1024 * 1024;

            // fixed seed for reproducable tests
            Random generator = new Random(13);

            byte buffer[] = new byte[len];
            generator.nextBytes(buffer);

            long read_start_time = System.currentTimeMillis();

            // use streamWrite for now:
            verbose(1, "streadWriting to:" + remoteFile);
            remoteFile.streamWrite(buffer, 0, buffer.length);
            long total_read_millis = System.currentTimeMillis()
                    - read_start_time;
            double speed = (len / 1024.0) / (total_read_millis / 1000.0);
            verbose(1, "write speed=" + ((int) (speed * 1000)) / 1000.0
                    + "KB/s");

            // move to remote (and do same basic asserts).
            verbose(1, "moving to local dir:" + localTempDir);
            localFile = remoteFile.moveTo(this.localTempDir);
            Assert.assertNotNull("new remote File is NULL", localFile);
            Assert.assertFalse(
                    "remote file reports it still exists, after it has moved",
                    remoteFile.exists());

            // get contents of localfile:
            byte newcontents[] = localFile.getContents();
            int newlen = newcontents.length;

            // check size:
            Assert.assertEquals(
                    "number of read bytes does not match file length", len,
                    newlen);

            // compare contents
            for (int i = 0; i < len; i++) {
                if (buffer[i] != newcontents[i]) {
                    Assert.assertEquals(
                            "Contents of file not the same. Byte nr=" + i,
                            buffer[i], newcontents[i]);
                }
            }

            localFile.delete();
        }

    }

    /**
     * Regression test for some stream write implementations: When writing to an
     * existing file, the existing file must be truncated when closing the
     * stream.
     *
     * @throws VlException
     */
    public void testStreamWriteTruncates() throws VlException {
        VFile remoteFile = null;
        remoteFile = getRemoteTestDir().createFile("testStreamWriteTruncate");

        int len = 10 * 1024; // 10k;

        // fixed seed for reproducable tests!
        Random generator = new Random(45);

        byte buffer[] = new byte[len];
        generator.nextBytes(buffer);
        // use streamWrite for now:
        verbose(1, "streadWriting to:" + remoteFile);
        remoteFile.streamWrite(buffer, 0, buffer.length);
        assertEquals("Initial remote file size NOT correct",
                remoteFile.getLength(), len);

        // reduce size!
        len = 5 * 1024; // new size less then 10k!
        buffer = new byte[len];
        generator.nextBytes(buffer);

        remoteFile.streamWrite(buffer, 0, buffer.length);

        verbose(1, "remoteFile.getLength()=" + remoteFile.getLength() + " len="
                + len);

        assertEquals("New file size should be truncated after Stream Write!",
                len, remoteFile.getLength());

        remoteFile.delete();
    }

    /**
     * Regression test whether file lengths always match the nr of written
     * bytes.
     *
     * @throws VlException
     */
    public void testMultiStreamWritesCheckLengths() throws VlException {
        // disabled for now :
        if (true) {
            return;
        }

        int numTries = 50;
        int maxSize = 1000;

        // Use own random generator but use fixed seed for deterministic
        // testing!
        Random generator = new Random(123123);
        VFile remoteFile = null;

        byte buffer[] = new byte[maxSize];
        for (int i = 0; i < numTries; i++) {
            int testSize = generator.nextInt(maxSize);
            remoteFile = getRemoteTestDir().createFile(
                    "testStreamWriteTest-" + i);

            generator.nextBytes(buffer);

            // use streamWrite for now:
            verbose(1, "streamWriting #" + testSize + " bytes to:" + remoteFile);

            remoteFile.streamWrite(buffer, 0, testSize);

            assertEquals("LFC File size NOT correct", remoteFile.getLength(),
                    testSize);

            if (remoteFile instanceof VReplicatable) {
                VRL reps[] = ((VReplicatable) remoteFile).getReplicas();
                if ((reps == null) || (reps.length <= 0)) {
                    fail("No replicas for Replicatable File!");
                }

                VFile rep = (VFile) remoteFile.getVRSContext().openLocation(
                        reps[0]);
                long repLen = rep.getLength();
                verbose(1,
                        "streamWriting #" + testSize + " replica (@"
                        + rep.getHostname() + ") length=" + repLen);
                assertEquals("LFC Replica File size NOT correct", repLen,
                        testSize);
            }

            remoteFile.delete();
        }
    }

    public void testCopyToRemote() throws VlException {
        testCopyMoveToRemote(false);
    }

    public void testMoveToRemote() throws VlException {
        testCopyMoveToRemote(true);
    }

    public void testCopyMoveToLocal(boolean isMove) throws VlException {
        VFile localFile = null;
        VFile remoteFile = null;

        remoteFile = getRemoteTestDir().createFile("testLocalFile");
        remoteFile.setContents(TEST_CONTENTS);

        if (isMove) {
            localFile = remoteFile.moveTo(localTempDir);
        } else {
            localFile = remoteFile.copyTo(localTempDir);
        }

        Assert.assertNotNull("new remote File is NULL", localFile);

        String str = localFile.getContentsAsString();
        Assert.assertEquals("Contents of local file not the same:" + str, str,
                TEST_CONTENTS);

        // file should be moved: remote file may not exist.
        if (isMove == true) {
            Assert.assertEquals("Remote file should not exist after move:"
                    + remoteFile, false, remoteFile.exists());
        }

        if (isMove == false) {
            remoteFile.delete();
        }

        localFile.delete();
    }

    public void testCopyToLocal() throws VlException {
        testCopyMoveToLocal(false);
    }

    public void testMoveToLocal() throws VlException {
        testCopyMoveToLocal(true);
    }

    /**
     * Test readRandomBytes first before testing random reads+writes
     */
    public void testRandomReadable() throws VlException {
        VFile localFile = this.localTempDir.createFile("readBytesFile1");
        int len = 16;
        byte orgBuffer[] = new byte[len];

        for (int i = 0; i < len; i++) {
            orgBuffer[i] = (byte) (i);
        }

        localFile.setContents(orgBuffer);
        VFile rfile = localFile.moveTo(getRemoteTestDir());
        if ((rfile instanceof VRandomReadable) == false) {
            message("===");
            message("Warning: Skipping readRandomBytes for file:" + rfile);
            message("===");

            return;
        }

        VRandomReadable vrfile = (VRandomReadable) rfile;

        _testRandomReadable(vrfile, 4, 8, orgBuffer);
        _testRandomReadable(vrfile, 8, 16, orgBuffer);
        _testRandomReadable(vrfile, 0, 1, orgBuffer);
        _testRandomReadable(vrfile, 1, 2, orgBuffer);
        _testRandomReadable(vrfile, 0, 5, orgBuffer);

        rfile.delete();

        //
        // 1MB size:
        //

        localFile = this.localTempDir.createFile("readBytesFile2");
        len = 1024 * 1024;
        orgBuffer = new byte[len];

        for (int i = 0; i < len; i++) {
            orgBuffer[i] = (byte) ((13 + len - i) % 256);
        }

        localFile.setContents(orgBuffer);
        rfile = localFile.moveTo(getRemoteTestDir());
        vrfile = (VRandomReadable) rfile;

        _testRandomReadable(vrfile, 4, 8, orgBuffer);
        _testRandomReadable(vrfile, 1, 2, orgBuffer);
        _testRandomReadable(vrfile, 0, 1, orgBuffer);
        _testRandomReadable(vrfile, 100001, 200002, orgBuffer);
        _testRandomReadable(vrfile, 200002, 222222, orgBuffer);
        _testRandomReadable(vrfile, 330002, 330256, orgBuffer);
        _testRandomReadable(vrfile, len - 1025, len, orgBuffer);

        rfile.delete();

    }

    private void _testRandomReadable(VRandomReadable rfile, int offset,
            int end, byte orgBuffer[]) throws VlException {
        byte readBuffer[] = new byte[orgBuffer.length];

        int numread = rfile.readBytes(offset, readBuffer, offset, end - offset);

        Assert.assertEquals("Couldn't read requested nr of bytes. numread="
                + numread, end - offset, numread);

        for (int i = offset; i < end; i++) {
            if (orgBuffer[i] != readBuffer[i]) {
                Assert.assertEquals("Contents of file not the same. Byte nr="
                        + i, orgBuffer[i], readBuffer[i]);
            }
        }
    }

    public void testVRandomReader() throws VlException {
        VFile localFile = this.localTempDir.createFile("RandomReaderFile");

        StringBuffer contents = new StringBuffer();
        for (int i = 0; i < 10; i++) {
            contents.append(i);
        }
        localFile.setContents(contents.toString());

        VRandomReader instance = new VRandomReader((VRandomReadable) localFile);

        // -----------Test length
        assertEquals(instance.length(), localFile.getLength());
        assertEquals(instance.length(), contents.length());

        // -----------Test Seek
        // read the first byte (0)
        int start = 0;
        int end = start + 1;
        instance.seek(start);
        byte[] signatureBytes = new byte[contents.substring(start, end).getBytes().length];
        instance.readFully(signatureBytes);
        assertEquals(contents.substring(start, end), new String(signatureBytes));

        // read the last two byte (9)
        start = instance.length() - 2;
        end = start + 1;
        instance.seek(start);
        signatureBytes = new byte[contents.substring(start, end).getBytes().length];
        instance.readFully(signatureBytes);
        assertEquals(contents.substring(start, end), new String(signatureBytes));

        // read byte in the midle(5)
        start = instance.length() / 2;
        end = start + 1;
        instance.seek(start);
        signatureBytes = new byte[contents.substring(start, end).getBytes().length];
        instance.readFully(signatureBytes);
        assertEquals(contents.substring(start, end), new String(signatureBytes));

        // -----------Test readBytes
        signatureBytes = new byte[1];
        instance.readBytes(0, signatureBytes, 0, signatureBytes.length);
        assertEquals(contents.substring(0, signatureBytes.length), new String(
                signatureBytes));

        // -----------Test skip
        // bring back to the start
        instance.seek(0);
        // skip 3 bytes
        int lenToSkip = 3;
        int skiped = instance.skipBytes(lenToSkip);
        signatureBytes = new byte[1];
        instance.readFully(signatureBytes);
        assertEquals(lenToSkip, skiped);
        assertEquals(contents.substring(lenToSkip, lenToSkip + 1), new String(
                signatureBytes));
    }

    /**
     * Writes 1,11,64k+ and 1MB+ number of bytes and reread them using
     * VFile.read() and VFile.write() Note: read() and write() are high level
     * method which use readBytes() and writeBytes() or streamRead and
     * streamWrite. The implementation chooses the read/write method.
     */
    public void testReadWriteBytes() throws VlException {
        if (getTestWriteTests() == false) {
            return;
        }

        VFile newFile = getRemoteTestDir().createFile("someBytes");

        // write single byte:

        byte buffer[] = new byte[]{13};
        testReadWriteBytes(newFile, 0, buffer);

        // write array of bytes;

        buffer = new byte[]{42, 13, 1, 2, 3, 4, 5, 6, 7, 8, 0, 9};
        testReadWriteBytes(newFile, 0, buffer);

        // >64k block
        int len = 65537;
        buffer = new byte[len];

        for (int i = 0; i < len; i++) {
            buffer[i] = (byte) ((13 + i) % 256);
        }

        testReadWriteBytes(newFile, 0, buffer);

        // >1MB block
        len = 1024 * 1024 + 13;
        buffer = new byte[len];

        for (int i = 0; i < len; i++) {
            buffer[i] = (byte) ((11 + i * 13 + i) % 256);
        }

        testReadWriteBytes(newFile, 0, buffer);
        newFile.delete();
    }

    /**
     * Writes 1,11,64k+ and 1MB+ number of bytes useing writeBytes() and
     * readBytes();
     *
     * @throws VlException
     *
     */
    public void testReadWriteRandomBytes() throws VlException {
        if (getTestWriteTests() == false) {
            return;
        }

        VFile newFile = getRemoteTestDir().createFile("randomBytes");
        VRandomAccessable randomWriter = null;

        if (newFile instanceof VRandomAccessable) {
            randomWriter = (VRandomAccessable) newFile;
        } else {
            message("File implementation doesn't support random write methods:"
                    + this);
            return;
        }

        // write small (#12) array of bytes;
        // include NEGATIVE number to test negative to postive
        byte buffer1[] = new byte[]{127, 42, 13, 1, 2, 3, 4, 5, 6, 7, 8, 0,
            9, -1, -10, -126, -127, -128};
        int nrBytes = buffer1.length;

        randomWriter.writeBytes(4, buffer1, 4, 4);
        randomWriter.writeBytes(8, buffer1, 8, 4);
        randomWriter.writeBytes(0, buffer1, 0, 4);
        // write remainder:
        randomWriter.writeBytes(12, buffer1, 12, nrBytes - 12);

        byte buffer2[] = new byte[nrBytes];

        // use syncReadBytes!
        int numRead = StreamUtil.syncReadBytes(randomWriter, 0, buffer2, 0,
                nrBytes);
        Assert.assertEquals("Number of actual read bytes is wrong!", nrBytes,
                numRead);

        for (int i = 0; i < nrBytes; i++) {
            if (buffer1[i] != buffer2[i]) {
                message("Read error. Buffer dump: ");

                for (int j = 0; j < nrBytes; j++) {
                    message("Buffer[" + j + "] 1,2 =" + buffer1[j] + ","
                            + buffer2[j]);
                }

                Assert.assertEquals("Contents of file not the same. Byte nr="
                        + i, buffer1[i], buffer2[i]);
            }
        }

        newFile.delete();
        newFile = getRemoteTestDir().createFile("randomBytes2");

        // use Pseudo random: make test reproducable !

        Random generator = new Random(0);

        int maxlen = 1024 * 1024 + 13;

        buffer1 = new byte[maxlen];
        buffer2 = new byte[maxlen];

        // write between 1-10% per write
        int minwrite = maxlen / 100;
        int maxwrite = maxlen / 10;

        // figure out average nr of writes needed to fill the file
        // (file doesn't need to be fully full)
        int nrWrites = maxlen / ((maxwrite - minwrite) / 2);

        if (nrWrites <= 0) {
            verbose(1, "nrwrites <=0");
        }

        for (int i = 0; i < nrWrites; i++) {
            int partlen = minwrite + generator.nextInt(maxwrite - minwrite);
            int offset = generator.nextInt(maxlen);

            // must not write over end of file:
            if (offset + partlen > maxlen) {
                partlen = maxlen - offset;
            }

            // generate random bytes:
            byte part[] = new byte[partlen];
            generator.nextBytes(part);

            // write bytes:
            randomWriter.writeBytes(offset, part, 0, partlen);

            // keep byte in buffer:
            for (int j = 0; j < partlen; j++) {
                buffer1[offset + j] = part[j];
            }

            verbose(1, "writeRandom:writes " + i + " of " + nrWrites);

        }

        // reread file contents: Use Sync READ !
        numRead = StreamUtil.syncReadBytes(randomWriter, 0, buffer2, 0, maxlen);
        Assert.assertEquals("Number of actual read bytes is wrong!", maxlen,
                numRead);

        // check readbuffer;

        for (int i = 0; i < maxlen; i++) {
            if (buffer1[i] != buffer2[i]) {
                Assert.assertEquals("Contents of file not the same. Byte nr="
                        + i, buffer1[i], buffer2[i]);
            }
        }

        newFile.delete();
    }

    private void testReadWriteBytes(VFile newFile, long offset, byte[] buffer1)
            throws VlException {
        VRandomAccessable randomWriter = null;

        if (newFile instanceof VRandomAccessable) {
            randomWriter = (VRandomAccessable) newFile;
        } else {
            message("File implementation doesn't support random write methods:"
                    + this);
            return;
        }

        byte buffer2[] = new byte[buffer1.length];

        int numBytes = buffer1.length;

        randomWriter.writeBytes(offset, buffer1, 0, numBytes);
        // newFile.sync(); => writeBytes is specified as synchronous !
        int numRead = randomWriter.readBytes(offset, buffer2, 0, numBytes);

        Assert.assertEquals(
                "Actual number of read bytes doesn't match request number of bytes!",
                numBytes, numRead);

        // check readbuffer;

        for (int i = 0; i < numBytes; i++) {
            if (buffer1[i] != buffer2[i]) {
                Assert.assertEquals("Contents of file not the same. Byte nr="
                        + i + " of " + numBytes, buffer1[i], buffer2[i]);
            }
        }

    }

    // not now:
	/*
     * public void testMultiThreadedRead() {
     *
     * }
     */
    public void testCopyDirToRemote() throws VlException {
        String subdirName = "subdir";

        VDir localTestDir = localTempDir.createDir(subdirName);

        int numFiles = 10;

        String fileNames[] = new String[numFiles];

        for (int i = 0; i < numFiles; i++) {
            fileNames[i] = "testFile" + i;
            VFile file = localTestDir.createFile(fileNames[i]);
            file.setContents(TEST_CONTENTS);
        }

        if (getRemoteTestDir().existsDir(localTestDir.getBasename())) {
            message("*** Warning: removing already existing target directory in:"
                    + getRemoteTestDir());
            // delete should already be test by now:
            getRemoteTestDir().getDir(localTestDir.getBasename()).delete(true);
        }

        debug("Move from: " + localTestDir.getVRL() + " to " + getRemoteLocation());
        VDir newRemoteDir = localTestDir.moveTo(getRemoteTestDir());

        boolean result = getRemoteTestDir().existsDir(subdirName);
        Assert.assertEquals("New remote directory doesn't exist (I):"
                + subdirName, true, result);

        result = newRemoteDir.exists();
        Assert.assertEquals("New remote directory doesn't exist (II):"
                + newRemoteDir, true, result);

        // check contents.
        for (int i = 0; i < numFiles; i++) {
            result = newRemoteDir.existsFile(fileNames[i]);
            Assert.assertEquals("Remote directory doesn't contain file:"
                    + fileNames[i], true, result);
        }

        try {
            newRemoteDir.delete(true);
        } catch (Exception e) {
            message("Warning: after VDir.delete(): ignoring exception as not part of unit test :"
                    + e);
        }
    }

    /**
     * Regression test to test wether a write and a read of a single unsigned
     * byte value > 128 will result in an integer value > 128. SRB had a bug
     * where a single (byte) read of a value > 128 resulted in a negative
     * integer value which is interpreted as a EOF (-1).
     *
     * @throws Exception
     */
    public void testStreamReadWriteSingleBytes() throws Exception {
        VFile remoteFile = null;

        // negative values should be auto-casted to their positive (usigned)
        // byte equivalents like in a cast
        // 0=0,1=0,127=127 AND -1 = 255, -2=254, etc,until -128=128 !
        // Also: for negative values <-128 the lowest significant byte is used
        // which
        // is a positive byte
        int values[] = {0, 1, 127, 128, 255, -1, -2, -126, -127, -128, -129,
            -130};
        // positive (usigned byte) values of above integers !
        int bytevals[] = {0, 1, 127, 128, 255, 255, 254, 130, 129, 128, 127,
            126};

        {
            remoteFile = this.getRemoteTestDir().createFile("single_byte", true);

            OutputStream outps = remoteFile.getOutputStream();

            for (int value : values) {
                outps.write(value);
            }
            outps.close();

            InputStream inps = remoteFile.getInputStream();

            for (int i = 0; i < bytevals.length; i++) {
                int val = inps.read();

                Assert.assertEquals(
                        "single byte read()+write() does not return expected value,",
                        bytevals[i], val);
            }

            inps.close();
        }
    }

    public void testSetLength() throws VlException {
        VFile file = getRemoteTestDir().createFile("testFileLength", true);

        Assert.assertEquals("New file must have zero length !", 0,
                file.getLength());

        if (file instanceof VZeroSizable) {
            ((VZeroSizable) file).setLengthToZero();
            Assert.assertEquals(
                    "after setLengthToZero: New file must still have zero length !",
                    0, file.getLength());
        } else {
            message("Skipping (not supported:) VZeroSizable.setLengthToZero()");
        }

        if (file instanceof VResizable) {
            ((VResizable) file).setLength(13);
            Assert.assertEquals(
                    "setLentgg() didn't increaze file size to new length", 13,
                    file.getLength());

            ((VResizable) file).setLength(3);
            Assert.assertEquals(
                    "setLentgg() didn't dencreaze file size to new length", 3,
                    file.getLength());

            ((VResizable) file).setLengthToZero();
            Assert.assertEquals(
                    "setLentgg() didn't decreaze file size to zero", 0,
                    file.getLength());

            int mil = 1024 * 1024;
            ((VResizable) file).setLength(mil);
            Assert.assertEquals(
                    "setLentgg() didn't increaze file size to new size", mil,
                    file.getLength());
        } else {
            message("Skipping (not supported:) VSizeAdjustable.setLength()");
        }
        file.delete();
    }

    private void _testStreamWrite(int targetSize) throws VlException {
        VFile file = getRemoteTestDir().createFile("streamWrite", true);
        // write 1MB buffer:
        byte[] buffer = new byte[targetSize];
        file.streamWrite(buffer, 0, buffer.length);

        long size = file.getLength();
        Assert.assertEquals(
                "testing write > 32k bug: Size of file after streamWrite not correct:"
                + size, size, targetSize);
        file.delete();
    }

    // ========================================================================
    // Test eXtra Resource Interfaces (Under construction)
    // ========================================================================
    public void testVUnixAttributes() throws VlException {
        if ((getRemoteTestDir().isLocal()) && (Global.isWindows())) {
            message("Skipping Unix atributes test for windows filesystem...");
            return;
        }

        VFile remoteFile = getRemoteTestDir().newFile("testUnixAttrs.txt");

        if (remoteFile instanceof VUnixFileAttributes) {
            VUnixFileAttributes uxFile = (VUnixFileAttributes) remoteFile;

            // write something.
            remoteFile.setContents(TEST_CONTENTS);

            String uid = uxFile.getUid();
            if (StringUtil.isEmpty(uid)) {
                Assert.fail("User ID (UID) is empty for (VUnixFileAttributes).getUid():"
                        + remoteFile);
            }

            String gid = uxFile.getGid();
            if (StringUtil.isEmpty(gid)) {
                Assert.fail("Group ID (GID) is empty for (VUnixFileAttributes).getGid():"
                        + remoteFile);
            }

            int mode = uxFile.getMode();
            // mode==0 is allowed!
            if (mode < 0) {
                Assert.fail("Unix File mode is negative for:" + remoteFile);
            }
        }

        if (remoteFile.exists()) {
            remoteFile.delete();
        }
    }

    public void testVCommentable() throws VlException {
        VFile file = getRemoteTestDir().createFile("testFile");

        if (file instanceof VCommentable) {

            VCommentable commentableFile = (VCommentable) file;

            Random r = new Random();
            String expected = "test VCommentable " + r.nextInt();
            commentableFile.setComment(expected);

            String actual = commentableFile.getComment();

            assertEquals(expected, actual);

            file.delete();

            try {
                // getting comments from non-existing file
                actual = commentableFile.getComment();
                // todo: exception test.
            } catch (ResourceNotFoundException ex) {
                Global.debugPrintln(this, "Caught expected Exception:" + ex);
                Global.debugPrintStacktrace(ex);
            }

        } else {
            file.delete();
        }

    }

    public void testVChecksum() throws VlException {
        VFile remoteFile = getRemoteTestDir().createFile("testChecksum.txt");

        remoteFile.setContents(TEST_CONTENTS);

        if (remoteFile instanceof VChecksum) {

            VChecksum checksumRemoteFile = (VChecksum) remoteFile;
            InputStream remoteIn = remoteFile.getInputStream();
            String[] types = checksumRemoteFile.getChecksumTypes();

            String calculated;
            String feached;
            for (int i = 0; i < types.length; i++) {
                message("Testing: " + types[i]);
                // check if both methods retun the same checksum
                calculated = ChecksumUtil.calculateChecksum(remoteIn, types[i]);
                feached = checksumRemoteFile.getChecksum(types[i]);
                message("-----------------calculated Checksum: " + calculated
                        + " feached Checksum: " + feached);
                assertEquals(calculated, feached);

                // now change the file and check if the checksum has also
                // chanded
                String initialChecksum = calculated;
                remoteFile.setContents("Changed contents");
                remoteFile = getRemoteTestDir().getFile("testChecksum.txt");

                feached = checksumRemoteFile.getChecksum(types[i]);
                assertNotSame(feached, initialChecksum);

                // download the file and compare it aginst the remote
                VFile result = remoteFile.copyTo(localTempDir);
                LFile localFile = (LFile) result;

                if (localFile instanceof VChecksum) {
                    VChecksum checksumLocalFile = (VChecksum) localFile;
                    checksumRemoteFile = (VChecksum) remoteFile;

                    message("Getting " + types[i] + " from local file");
                    String localChecksum = checksumLocalFile.getChecksum(types[i]);
                    message("Getting " + types[i] + " from remote file");
                    String remoteChecksum = checksumRemoteFile.getChecksum(types[i]);

                    message("localChecksum: " + localChecksum
                            + " remoteChecksum: " + remoteChecksum);
                    assertEquals(localChecksum, remoteChecksum);
                }
                // Check exception
                try {
                    String remoteChecksum = checksumRemoteFile.getChecksum("NON EXISTING ALGORITHM");

                    message("Checksum: " + remoteChecksum);
                } catch (Exception ex) {
                    if (!(ex instanceof nl.uva.vlet.exception.NotImplementedException)) {
                        fail("Should throw NotImplementedException. Instead got back "
                                + ex.getMessage());
                    } else {
                        message("Correct exeption!!: " + ex.getMessage());
                    }
                }
                localFile.delete();
            }
        }

    }

    public void testVReplicatable() throws VlException {

        String fileName = "testReplicable.txt";
        VFile remoteFile = null;

        // if file has no replic create one
        if (!getRemoteTestDir().existsFile(fileName)) {
            remoteFile = getRemoteTestDir().createFile(fileName);
            remoteFile.setContents("Test contents");
        } else {
            remoteFile = getRemoteTestDir().getFile(fileName);
        }

        if ((remoteFile instanceof VReplicatable) == false) {
            // delete?
            return;
        }
        VReplicatable replicable = (VReplicatable) remoteFile;
        long len = remoteFile.getLength();

        VRL[] replicas = replicable.getReplicas();
        ITaskMonitor monitor = ActionTask.getCurrentThreadTaskMonitor(
                "Test VReplicable:", -1);

        // Keep only one replica
        for (int i = 0; i < replicas.length; i++) {
            message("Registered replica VRLs: " + replicas[i]);
            if (i >= 1) {
                replicable.deleteReplica(monitor, replicas[i].getHostname());
            }
        }

        BdiiService service = VRSContext.getDefault().getBdiiService();

        // get all se
        ArrayList<StorageArea> se = service.getSRMv22SAsforVO(VRSContext.getDefault().getVO());

        VFSClient vfs = new VFSClient();

        unregisterEmptyRep(replicable, vfs);

        VRL replicaVRL;
        VFile replicaFile;
        boolean success = false;
        StringList blackListedSEs = new StringList(TestSettings.BLACK_LISTED_SE);

        // replacate to all except rug
        for (int i = 0; i < se.size(); i++) // for (int i = 0; i < 3; i++)
        {
            String host = se.get(i).getHostname();

            // skip original replica
            if (se.get(i).getHostname().equals(replicas[0].getHostname())) {
                continue;
            }

            if (blackListedSEs.contains(host) == false) {
                message("Storage Element: " + se.get(i).getHostname());
                message("   Replicating");
                replicaVRL = replicable.replicateTo(monitor, se.get(i).getHostname());
                replicaFile = vfs.getFile(replicaVRL);
                message("   Replecated to: " + replicaFile);

                // ----Check if it is correctly replicated-------------
                // is file created ??
                boolean exists = replicaFile.exists();
                message("   Exists?: " + exists);
                assertTrue(exists);
                // is the same size??
                long replicaLen = replicaFile.getLength();
                message("   Original length " + len + " replica lenght: "
                        + replicaLen);
                assertEquals(len, replicaLen);

                // checksum
                if ((replicaFile instanceof VChecksum)
                        && (remoteFile instanceof VChecksum)) {
                    String replicaChecksome = ((VChecksum) replicaFile).getChecksum(VChecksum.MD5);
                    String remoteFileChecksome = ((VChecksum) remoteFile).getChecksum(VChecksum.MD5);
                    assertEquals(remoteFileChecksome, replicaChecksome);
                }

                // -----------------------DELETE----------------------------
                message("   Deleteing");
                success = replicable.deleteReplica(monitor, se.get(i).getHostname());
                assertTrue(success);
                exists = replicaFile.exists();
                message("   Exists?: " + exists);
                assertFalse(exists);
            } else {
                message("Ooops it's " + se.get(i).getHostname()
                        + " don't replicate there");
            }
        }

        // test Exception
        try {
            replicaVRL = replicable.replicateTo(monitor, "NON-EXISTING-SE");
            message("New replica!!: " + replicaVRL);
            fail("Replicating to UNKNOWN storage element should fail");
        } catch (Exception ex) {
            if (!(ex instanceof ResourceCreationFailedException)) {
                fail("Wrong exeption. Should be ResourceCreationFailedException instead got: "
                        + ex);
            } else {
                message("Got back correct exception: " + ex);
            }
        }

        // get back the replicas VRL
        replicas = replicable.getReplicas();

        message("Listing replicas.........");
        for (int i = 0; i < replicas.length; i++) {
            message("Registered replica VRLs: " + replicas[i]);
        }

        // Test Register
        int VRllen = 4;
        VRL[] vrls = new VRL[VRllen];
        // warning!!! if you include port number LFC will remove it
        for (int i = 0; i < VRllen; i++) {
            vrls[i] = new VRL("scheme://host.at.some.domain/path/to/file" + i);
        }

        success = replicable.registerReplicas(vrls);
        assertTrue(success);

        // get back the replicas VRL
        replicas = replicable.getReplicas();
        List<VRL> vrlList = Arrays.asList(replicas);

        message("Listing replicas.........");
        for (int i = 0; i < replicas.length; i++) {
            message("Registered replica VRLs: " + replicas[i]);
            if (i < vrls.length) {
                // message("Is " + vrls[i] + " contained in vrlList?");
                // check if registered vrls are there
                if (!vrlList.contains(vrls[i])) {
                    fail("Didn't get back the same VRLs from the service. "
                            + replicas[i]
                            + " is not contained in the registered VRS");
                }
            }

        }

        message("Unregistering replicas.........");
        success = replicable.unregisterReplicas(vrls);
        assertTrue(success);

        replicas = replicable.getReplicas();
        vrlList = Arrays.asList(replicas);

        message("Listing replicas.........");
        for (int i = 0; i < replicas.length; i++) {
            message("Registered replica VRLs: " + replicas[i]);
            if (i < vrls.length) {
                // check if unregistered vrls are gone
                if (vrlList.contains(vrls[i])) {
                    fail("Didn't remove VRLs. " + replicas[i]
                            + " is contained in the registered VRS");
                }
            }

        }
        // clean up
        success = remoteFile.delete();
        assertTrue(success);

    }

    private static void unregisterEmptyRep(VReplicatable rep, VFSClient vfs) {
        VRL[] replicas = null;
        if (vfs == null) {
            vfs = new VFSClient();
        }
        VFile file = null;
        ArrayList<VRL> emptyRep = new ArrayList<VRL>();
        try {
            replicas = rep.getReplicas();
        } catch (VlException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        message("Listing replicas.........");
        for (int i = 0; i < replicas.length; i++) {

            try {
                // file = vfs.newFile(replicas[i]);
                file = vfs.getFile(replicas[i]);
                if (!file.exists()) {
                    message("Replica VRL: " + replicas[i]
                            + " doesn't exist. Unregistering");
                    emptyRep.add(replicas[i]);
                }
            } catch (VlException e) {
                message("Replica VRL: " + replicas[i]
                        + " doesn't exist. Unregistering");
                emptyRep.add(replicas[i]);
            }
        }

        VRL[] emptyRepArray = new VRL[emptyRep.size()];
        emptyRepArray = emptyRep.toArray(emptyRepArray);
        try {
            rep.unregisterReplicas(emptyRepArray);
            replicas = rep.getReplicas();
        } catch (VlException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        message("Listing replicas.........");
        for (int i = 0; i < replicas.length; i++) {
            message("Remaing replicas: " + replicas[i]);
        }

    }

    public void testX3rdPartySameServer() throws VlException {
        VRL loc1 = getRemoteLocation();

        VRL loc2 = getRemoteLocation();

        // same location but different names
        testX3rdPartyTransfer(loc1, loc2, "3rdPartyFile1", "3rdPartyFile2",
                true);

    }

    public void testX3rdPartySameServerReverse() throws VlException {
        VRL loc1 = getRemoteLocation();

        VRL loc2 = getRemoteLocation();

        // same location but different names
        testX3rdPartyTransfer(loc1, loc2, "3rdPartyFile3", "3rdPartyFile4",
                false);

    }

    public void testX3rdPartyTwoServers() throws VlException {
        VRL loc1 = getRemoteLocation();

        VRL loc2 = getOtherRemoteLocation();
        if (loc2 == null) {
            message("testX3rdPartyTwoServers: skipping 3rd party test: No other location configured");
            return;
        }
        // same location but different names
        testX3rdPartyTransfer(loc1, loc2, "3rdPartyFile5", "3rdPartyFile6",
                true);

    }

    public void testX3rdPartyTwoServersReverse() throws VlException {
        VRL loc1 = getRemoteLocation();

        VRL loc2 = getRemoteLocation();
        if (loc2 == null) {
            message("testX3rdPartyTwoServers: skipping 3rd party test: No other location configured");
            return;
        }
        // same location but different names
        testX3rdPartyTransfer(loc1, loc2, "3rdPartyFile7", "3rdPartyFile8",
                false);

    }

    // // need two different test locations for this:
    // public void test2Servers() throws VlException
    // {
    // VRL loc1=TestSettings.testGFTPLocation;
    // VRL loc2=TestSettings.testGFTPLocation2;
    //
    // // different locations
    // testX3rdPartyTransfer(loc1,loc2,"file1","file2");
    // }
    //
    // public void test2ServersReverse() throws VlException
    // {
    // VRL loc1=TestSettings.testGFTPLocation;
    // VRL loc2=TestSettings.testGFTPLocation2;
    //
    // // different locations
    // testX3rdPartyTransfer(loc2,loc1,"file3","file4");
    // }
    public void testX3rdPartyTransfer(VRL sourceDir, VRL targetDir,
            String sourceFilename, String targetFilename,
            boolean sourceIsActiveParty) throws VlException {
        VDir dir1 = getVFS().mkdir(sourceDir, true);
        VDir dir2 = getVFS().mkdir(targetDir, true);

        String contents = "Test 3rd party transfer\n";

        VFile file1 = dir1.createFile(sourceFilename);

        file1.setContents(contents);
        VRL sourceVRL = file1.getVRL();
        VRL targetVRL = targetDir.append(targetFilename);
        // create new VFile object, but do not really create file on filesystem
        // !
        VFile newTargetFile = dir2.getFileSystem().newFile(targetVRL);

        StringHolder explanation = new StringHolder();

        boolean skipTest = false;

        if (sourceIsActiveParty) {
            if ((file1 instanceof VThirdPartyTransferable) == false) {
                message("Skipping test: Third party transfers not supported by source file:"
                        + file1);
                skipTest = true;
            } // fail("GridFTP file should be VThirdPartyTransferable resources");
            else {
                boolean check = ((VThirdPartyTransferable) file1).canTransferTo(targetVRL, explanation);
                if (check == false) {
                    message("Skipping test: Third party transfer not possible from source to target:"
                            + targetVRL);
                    skipTest = true;
                    // Assert.assertTrue("A GFTP should always be able to do 3rd party transfers",check);
                }
            }
        } else {
            if ((newTargetFile instanceof VThirdPartyTransferable) == false) {
                message("Skipping test: Third party transfer not possible by target file:"
                        + newTargetFile);
                skipTest = true;
            } // fail("GridFTP file should be VThirdPartyTransferable resources");
            else {
                boolean check = ((VThirdPartyTransferable) newTargetFile).canTransferFrom(sourceVRL, explanation);
                if (check == false) {
                    message("Skipping test: Third party transfer (target <= source), not possible from source:"
                            + sourceVRL);
                    skipTest = true;
                    // Assert.assertTrue("A GFTP should always be able to do 3rd party transfers",check);
                }
            }
        }

        if (skipTest == true) {
            file1.delete();
            return;
        }

        // perform transfer
        VFile resultFile = null;

        DefaultTaskMonitor monitor = new DefaultTaskMonitor();

        if (sourceIsActiveParty) {
            resultFile = ((VThirdPartyTransferable) file1).activePartyTransferTo(monitor, targetVRL);
        } else {
            resultFile = ((VThirdPartyTransferable) newTargetFile).activePartyTransferFrom(monitor, sourceVRL);
        }

        Assert.assertNotNull("Result of 3rd party transfer can not be null.",
                newTargetFile);
        Assert.assertTrue(
                "Resulting new  file should report that it does exists now",
                newTargetFile.exists());

        // cleanup:

        file1.delete();
        newTargetFile.delete();

    }

    // public void test3rdPartyNotSupported() throws VlException
    // {
    // VFile file1=this.remoteTestDir.createFile("dummyFile123");
    //
    // if ((file1 instanceof VThirdPartyTransferable)==false)
    // {
    // fail("GridFTP file should be VThirdPartyTransferable resource");
    // }
    //
    // VRL remoteLocation=new VRL("http://www.vl-e.nl/dummypath");
    // StringHolder explanation=new StringHolder();
    //
    // boolean
    // check=((VThirdPartyTransferable)file1).canTransferTo(remoteLocation,
    // explanation);
    // message("3rd party canTransferTo() method returned:"+explanation.value);
    // Assert.assertFalse("GridFTP should be not be able to party transfers to HTTP locations",check);
    //
    //
    // }
    // ========================================================================
    // Explicit Regression Tests (Start with 'Z' to show up last in eclipse)
    // ========================================================================
    /**
     * Regression test for SFTP:
     *
     * When writing to a file, last write must be 32k or else the write will not
     * complete...
     *
     * @param targetSize
     * @throws VlException
     */
    public void testZRegressionStreamWrite32KBug() throws VlException {
        _testStreamWrite(32000); // this worked
        _testStreamWrite(1024 * 1024); // this didn't work
    }

    // ========================================================================
    // Explicit Exception Tests for robuZt programming !
    // ========================================================================
    /**
     * API test for atomic file creation.
     */
    // junit 4: @Test(expected=ResourceAlreadyExistsException.class)
    public void testZExceptionCreateFileDoNotIgnoreExisting()
            throws VlException {
        VFile newFile = getRemoteTestDir().createFile("testFile1");

        // current implemenation is to ignore existing files
        // except when force=false

        try {
            newFile = getRemoteTestDir().createFile("testFile1", false);
            fail("Should raise at least an Exception ");
        } catch (Exception e) {
            ; // ok
        }

        newFile.delete();
    }

    /**
     * API test for atomic directory creation.
     */
    public void testZExceptionCreateDirNotIgnoreExsting() throws VlException {
        VDir newDir = getRemoteTestDir().createDir("testDir1");
        try {
            newDir = getRemoteTestDir().createDir("testDir1", false);
            fail("Should raise and Exception. Preferably:"
                    + ResourceAlreadyExistsException.class);
        } catch (Exception e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            Global.debugPrintStacktrace(e);
        }

        newDir.delete();
    }

    public void testZCreateDirectoryWhileFileWithSameNameExists()
            throws VlException {
        String fdname = "testfiledir2";

        if (getRemoteTestDir().existsDir(fdname)) {
            // previous test went wrong !!
            verbose(0,
                    "*** Warning: Remote testfile already exists and is a directory");
            VDir dir = getRemoteTestDir().getDir(fdname);
            dir.delete(false);
            assertFalse(
                    "Could not remote previous test directory. Please remove it manually:"
                    + dir, dir.exists());
            // fail("Remote testfile is already directory. Please remove previous test directory!");
        }

        VFile newfile = getRemoteTestDir().createFile("testfiledir2");

        // MUST return false!
        assertFalse(
                "existsDir() must return FALSE when file with same name already exists!",
                getRemoteTestDir().existsDir("testfiledir2"));

        try {
            VDir newDir = getRemoteTestDir().createDir("testfiledir2");
            fail("Create directory out of existing file should raise Exception:");
        } // both are allowed:
        catch (ResourceCreationFailedException e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            Global.debugPrintStacktrace(e);
        } catch (ResourceAlreadyExistsException e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            Global.debugPrintStacktrace(e);
        }

        newfile.delete();
    }

    public void testZCreateFileWhileDirectoryWithSameNameExists()
            throws VlException {
        VDir newDir = getRemoteTestDir().createDir("testfiledir3");

        // MUST return false!
        assertFalse(
                "existsFile() must return FALSE when directory with same name already exists!",
                getRemoteTestDir().existsFile("testfiledir3"));
        try {
            VFile newfile = getRemoteTestDir().createFile("testfiledir3");
            fail("Create file out of existing directory should raise Exception:");
        } // both are allowed:
        catch (ResourceCreationFailedException e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            Global.debugPrintStacktrace(e);
        } catch (ResourceAlreadyExistsException e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            Global.debugPrintStacktrace(e);
        }
        {
            newDir.delete();
        }
    }

    // junit 4: @Test(expected=ResourceAlreadyExistsException.class)
    public void testZExceptionsExistingFile() throws VlException {
        VFile newFile = getRemoteTestDir().createFile("testExistingFile1");

        // current implemenation is to ignore existing files
        // except when force=false

        try {
            // create and do NOT ignore:
            newFile = getRemoteTestDir().createFile("testExistingFile1", false);
            fail("createFile(): Should raise at least an ResourceExistsException ");
        } catch (ResourceAlreadyExistsException e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            Global.debugPrintStacktrace(e);
        }

        newFile.delete();

        // Check create File while Dir exists:
        VDir newDir = getRemoteTestDir().createDir("testExistingDir1");

        try {
            // create and do NOT ignore:
            newFile = getRemoteTestDir().createFile("testExistingDir1", false);
            newFile.delete();

            fail("createFile(): Should raise at least an ResourceExistsException ");
        } // also allowed as the intended resource doesn't exists as exactly
        // the same type: existing Directory is not the intended File
        catch (ResourceCreationFailedException e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            Global.debugPrintStacktrace(e);
        } catch (ResourceAlreadyExistsException e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            Global.debugPrintStacktrace(e);
        }

        newDir.delete();
    }

    public void testZExceptionsExistingDir() throws VlException {
        VDir newDir = getRemoteTestDir().createDir("testExistingDir2");

        try {
            // create and do NOT ignore:
            newDir = getRemoteTestDir().createDir("testExistingDir2", false);
            newDir.delete();
            fail("createDir(): Should raise Exception:"
                    + ResourceAlreadyExistsException.class);
        } catch (ResourceAlreadyExistsException e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            Global.debugPrintStacktrace(e);
        }

        newDir.delete();
        VFile newFile = getRemoteTestDir().createFile("testExistingFile2");

        try {
            // create Dir and do NOT ignore existing File or Dir:
            newDir = getRemoteTestDir().createDir("testExistingFile2", false);
            newDir.delete();
            fail("createDir(): Should raise Exception:"
                    + ResourceAlreadyExistsException.class + " or "
                    + ResourceCreationFailedException.class);
        } // also allowed as the intended resource doesn't exists as exactly
        // the same type: existing Directory is not the intended File
        catch (ResourceCreationFailedException e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            // Global.debugPrintStacktrace(e);
        } catch (ResourceAlreadyExistsException e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            // Global.debugPrintStacktrace(e);
        } catch (ResourceException e) {
            Global.debugPrintln(this, "Caugh expected Exception:" + e);
            fail("createDir(): Although a resource execption is better then any other,"
                    + "this unit test expects either:"
                    + ResourceAlreadyExistsException.class
                    + " or "
                    + ResourceCreationFailedException.class);
            // Global.debugPrintStacktrace(e);
        }

        newFile.delete();
    }

    public void testUpDownloadLargeFile() throws VlException, IOException {

        VFile newFile = getRemoteTestDir().createFile("tesLargeFile");
        byte[] randomData = new byte[1024 * 1024];//1MB
        Random r = new Random();
        OutputStream lfos = newFile.getOutputStream();
        int count = 50;

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            r.nextBytes(randomData);
            lfos.write(randomData);
        }

        lfos.flush();
        lfos.close();
        long totalTime = System.currentTimeMillis() - startTime;


        long len = newFile.getLength();
        double up_speed = (len / 1024.0) / (totalTime / 1000.0);
        verbose(1, "upload speed=" + ((int) (up_speed * 1000)) / 1000.0
                + "KB/s");


        long expectedLen = randomData.length * count;
        assertEquals(expectedLen, len);

        startTime = System.currentTimeMillis();
        VFile newLocalFile = newFile.moveTo(this.localTempDir, "testLargeFile");

        totalTime = System.currentTimeMillis() - startTime;

        double down_speed = (newLocalFile.getLength() / 1024.0) / (totalTime / 1000.0);
        verbose(1, "download speed=" + ((int) (down_speed * 1000)) / 1000.0
                + "KB/s");

        Assert.assertNotNull("new local File is NULL", newLocalFile);
        Assert.assertFalse("remote file reports it still exists, after it has moved", newFile.exists());

        newLocalFile.delete();

        //Now test lots of small
        startTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            r.nextBytes(randomData);
            getRemoteTestDir().createFile("tesLargeFile" + i).setContents(randomData);
        }

        totalTime = System.currentTimeMillis() - startTime;
        up_speed = ((count * randomData.length) / 1024.0) / (totalTime / 1000.0);
        verbose(1, "Many files upload speed=" + ((int) (up_speed * 1000)) / 1000.0
                + "KB/s");


        for (VFSNode n : getRemoteTestDir().getNodes()) {
            n.delete();
        }

    }
    // ========================================================================
    // Abstract Interface
    // ========================================================================
    // VFS methods which must be subclassed by VFS Implementation.

    /**
     * For 3rd party transfer !
     */
    public VRL getOtherRemoteLocation() {
        return this.otherRemoteLocation;
    }

    protected void setTestRenames(boolean doRename) {
        this.testRenames = doRename;
    }

    protected void setTestStrangeChars(boolean testStrange) {
        this.testStrangeChars = testStrange;
    }

    protected void setTestWriteTests(boolean doWrites) {
        this.doWrites = doWrites;
    }

    protected boolean getTestWriteTests() {
        return doWrites;
    }

    protected void setTestDoBigTests(boolean doBigTests) {
        this.doBigTests = doBigTests;
    }

    protected boolean getTestDoBigTests() {
        return doBigTests;
    }

    /**
     * LAST UNIT TEST: Cleanup test directories
     */
    public void testZZZRemoveTestDir() {
        try {
            this.getRemoteTestDir().delete(true);
        } catch (Exception e) {
            message("*** Warning. Deleting remote test directory failed:" + e);
        }

        try {
            this.localTempDir.delete(true);
        } catch (Exception e) {
            message("*** Warning. Deleting local test directory failed:" + e);
        }
    }

    public void setRemoteTestDir(VDir remoteTestDir) {
        this.remoteTestDir = remoteTestDir;
    }

    public VDir getRemoteTestDir() {
        return remoteTestDir;
    }

    protected void setOtherRemoteLocation(VRL otherRemoteLocation) {
        this.otherRemoteLocation = otherRemoteLocation;
    }
}
