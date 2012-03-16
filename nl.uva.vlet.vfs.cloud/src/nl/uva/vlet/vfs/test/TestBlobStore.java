/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva.vlet.vfs.test;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import nl.uva.vlet.data.StringUtil;
import nl.uva.vlet.exception.ResourceAlreadyExistsException;
import nl.uva.vlet.exception.ResourceCreationFailedException;
import nl.uva.vlet.exception.ResourceException;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.blobstore.options.ListContainerOptions.Builder;

/**
 *
 * @author skoulouz
 */
public class TestBlobStore {

    private static AsyncBlobStore asyncBlobStore;

    public static void main(String args[]) {
        try {
            setup();
//        testToch();
//            touch(true);
            mkdir(true);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            asyncBlobStore.getContext().close();
        }

    }

    private static void testToch() throws InterruptedException, ExecutionException, IOException {


        String[] containerAndPath = new String[]{"testBlobStoreVFS", "testReplicable.txt"};


        Blob blob = asyncBlobStore.blobBuilder(containerAndPath[1]).payload("data2").build();

        ListenableFuture<String> res = asyncBlobStore.putBlob(
                containerAndPath[0], blob);


        String get = res.get();
        ListenableFuture<Blob> theBlob = asyncBlobStore.getBlob(containerAndPath[0], containerAndPath[1]);
        blob = theBlob.get();
        InputStream ins = blob.getPayload().getInput();
        byte[] data = new byte[1024];
        ins.read(data);
        ins.close();

        debug("Content: " + new String(data));

//        blob = asyncBlobStore.blobBuilder(containerAndPath[1]).payload("").build();
//        get = res.get();

    }

    public static boolean touch(boolean ignoreExisting) throws InterruptedException, Exception {

//        String[] containerAndPath = new String[]{"testBlobStoreVFS", "testReplicable.txt"};
//        String[] containerAndPath = new String[]{"testBlobStoreVFS", "testDirB0"};
//        String[] containerAndPath = new String[]{"testBlobStoreVFS", "testDirH"};
//        String[] containerAndPath = new String[]{"testBlobStoreVFS", "dir"};
        String[] containerAndPath = new String[]{"testBlobStoreVFS", "nonExisting"};

        //Exists ?
        ListenableFuture<BlobMetadata> blobMeta = asyncBlobStore.blobMetadata(containerAndPath[0], containerAndPath[1]);
        BlobMetadata meta = blobMeta.get();
        if (meta == null) {
            //Ok non existing 
            Blob blob = asyncBlobStore.blobBuilder(containerAndPath[1]).type(StorageType.BLOB).build();
            blob.setPayload("");
            ListenableFuture<String> res = asyncBlobStore.putBlob(containerAndPath[0], blob);

            res.get();

            return true;
        }
        //What is it?
        if (meta.getType() != StorageType.BLOB) {
            throw new Exception(containerAndPath[1] + " already exists as a folder");
        } else if (meta.getType() == StorageType.BLOB && !ignoreExisting) {
            throw new Exception(containerAndPath[1] + " already exists");
        } else if (meta.getType() == StorageType.BLOB && ignoreExisting) {
            return true;
        }
        return false;
    }

    protected static void mkdir(boolean ignoreExisting) throws Exception {

        String[] containerAndPath = new String[]{"testBlobStoreVFS", "dir3"};
        //Create container
        if (containerAndPath.length <= 1
                || StringUtil.isEmpty(containerAndPath[1])) {
            createContainer(containerAndPath[0], ignoreExisting);
        } else {
            createFolder(containerAndPath, ignoreExisting);
        }
    }

    private static void setup() throws FileNotFoundException, IOException {
        String provider = "swift"; // "in-memory"
//        Properties props = new Properties();

        Properties props = getCloudProperties();

        if (StringUtil.isEmpty(provider)) {
            throw new NullPointerException("Provider is null!");
        }
        String endpoint = "https://149.156.10.131:8443/auth/v1.0/testBlobStoreVFS";

        props.setProperty(org.jclouds.Constants.PROPERTY_ENDPOINT, endpoint);
        props.setProperty(org.jclouds.Constants.PROPERTY_TRUST_ALL_CERTS,
                "true");
        props.setProperty(org.jclouds.Constants.PROPERTY_RELAX_HOSTNAME, "true");

        String path = System.getProperty("user.home") + "/.local/filesystemstorage";
        File localStorage = new File(path);
        if (!localStorage.exists()) {
            localStorage.mkdirs();

        }
        props.setProperty(FilesystemConstants.PROPERTY_BASEDIR, path);

        BlobStoreContext blobStoreContext = new BlobStoreContextFactory().createContext(provider, props);
        asyncBlobStore = blobStoreContext.getAsyncBlobStore();
    }

    private static void debug(String msg) {
        System.err.println(TestBlobStore.class.getName() + ": " + msg);
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

    private static void createContainer(String container, boolean ignoreExisting) throws ResourceAlreadyExistsException, InterruptedException, ExecutionException, ResourceCreationFailedException {
        ListenableFuture<Boolean> res = asyncBlobStore.containerExists(container);
        Boolean containerExists = res.get();
        if (containerExists && !ignoreExisting) {
            throw new nl.uva.vlet.exception.ResourceAlreadyExistsException(
                    container + "Exists");
        } else if (containerExists) {
            //return new CloudDir(this, vrl);
            return;
        }
        ListenableFuture<Boolean> createContainer = asyncBlobStore.createContainerInLocation(null,
                container);
        Boolean created = createContainer.get();

        if (!created) {
            throw new nl.uva.vlet.exception.ResourceCreationFailedException(
                    "Could not create " + container);
        }
        //return new CloudDir(this, vrl);
        return;
    }

    private static void createFolder(String[] containerAndPath, boolean ignoreExisting) throws ExecutionException, InterruptedException, ResourceAlreadyExistsException, ResourceException {

        //Exists ?
        ListenableFuture<BlobMetadata> blobMeta = asyncBlobStore.blobMetadata(containerAndPath[0], containerAndPath[1]);
        BlobMetadata meta = blobMeta.get();
        if (meta == null) {
            //Ok non existing
            try {
                ListenableFuture<Void> createdDirRes = asyncBlobStore.createDirectory(containerAndPath[0], containerAndPath[1]);
                createdDirRes.get();
                //return new CloudDir(this, vrl);
                return;
            } catch (Exception ex) {
                if (!ex.getMessage().contains("(Is a directory)")) {
                    if (ex instanceof ResourceAlreadyExistsException) {
                        throw ((ResourceAlreadyExistsException) ex);
                    }
                    throw new ResourceException(ex.getMessage());
                }
            }
        }
        //What is it?
        if (meta.getType() == StorageType.BLOB) {
            throw new nl.uva.vlet.exception.ResourceAlreadyExistsException(
                    containerAndPath[1] + " already exists as a file");
        }
        if (!ignoreExisting) {
            throw new nl.uva.vlet.exception.ResourceAlreadyExistsException(
                    containerAndPath[1] + "Exists");
        }
        //return new CloudDir(this, vrl);
    }
}
