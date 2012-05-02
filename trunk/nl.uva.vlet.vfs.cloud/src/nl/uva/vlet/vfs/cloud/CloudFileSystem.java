package nl.uva.vlet.vfs.cloud;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import java.util.logging.Logger;
import nl.uva.vlet.ClassLogger;
import nl.uva.vlet.data.StringUtil;
import nl.uva.vlet.exception.ResourceException;
import nl.uva.vlet.exception.ResourceNotFoundException;
import nl.uva.vlet.exception.VRLSyntaxException;
import nl.uva.vlet.exception.VlException;
import nl.uva.vlet.exception.VlInitializationException;
import nl.uva.vlet.exception.VlPasswordException;
import nl.uva.vlet.vfs.FileSystemNode;
import nl.uva.vlet.vfs.VDir;
import nl.uva.vlet.vfs.VFSNode;
import nl.uva.vlet.vfs.VFile;
import nl.uva.vlet.vfs.cloud.Exceptions.CloudRequestTimeout;
import nl.uva.vlet.vrl.VRL;
import nl.uva.vlet.vrs.ServerInfo;
import nl.uva.vlet.vrs.VRSContext;

import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.domain.internal.StorageMetadataImpl;
import org.jclouds.blobstore.options.ListContainerOptions.Builder;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.File;
import java.util.logging.Level;
import nl.uva.vlet.Global;
import nl.uva.vlet.exception.ResourceAlreadyExistsException;
import nl.uva.vlet.exception.ResourceCreationFailedException;
import nl.uva.vlet.exception.VlIOException;
import org.jclouds.filesystem.reference.FilesystemConstants;

/**
 *
 *
 *
 * @author S. Koulouzis.
 *
 */
public class CloudFileSystem extends FileSystemNode {

    private String provider;
    private Properties props;
    private String prefixPath = "/auth/v1.0";
    static ClassLogger logger;

    {
        try {
            logger = ClassLogger.getLogger(CloudFileSystem.class);
            logger.setLevelToDebug();
            logger.setLevel(Level.ALL);
        } catch (SecurityException ex) {
            Logger.getLogger(CloudFileSystem.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    private AsyncBlobStore asyncBlobStore;

    public CloudFileSystem(VRSContext context, ServerInfo info)
            throws VlInitializationException, VlPasswordException, VRLSyntaxException, VlIOException {
        super(context, info);
        provider = info.getScheme();// "swift";// "in-memory"
        props = new Properties();

        if (StringUtil.isEmpty(provider)) {
            throw new NullPointerException("Provider is null!");
        }

        VRL serverVRL = info.getServerVRL();
        if (!serverVRL.getScheme().equals(CloudConstants.FILESYSTEM_SCHME)) {

            String endpoint = serverVRL.copyWithNewScheme("https").toString();
            debug("Endpoint: " + endpoint);

            if (StringUtil.isEmpty(endpoint)) {
                throw new nl.uva.vlet.exception.VlInitializationException(
                        "Cloud service endpoint is null");
            }

            props.setProperty(org.jclouds.Constants.PROPERTY_ENDPOINT, endpoint);
            info.setAuthScheme(ServerInfo.PASSWORD_OR_PASSPHRASE_AUTH);
        }

        String username = info.getUsername();
        if (StringUtil.isEmpty(username)) {
            throw new NullPointerException("Username is null!");
        }

        debug("Username: " + username);
        String passwd = info.getPassword();
        debug("Password: " + passwd);
        if (StringUtil.isEmpty(passwd)) {
            debug("password is empty!!!");
            throw new nl.uva.vlet.exception.VlPasswordException(
                    "Cloud service credential (password) is null");
        }

        props.setProperty(org.jclouds.Constants.PROPERTY_IDENTITY, username);
        props.setProperty(org.jclouds.Constants.PROPERTY_CREDENTIAL, passwd);
        props.setProperty(org.jclouds.Constants.PROPERTY_TRUST_ALL_CERTS,
                "true");

        props.setProperty(org.jclouds.Constants.PROPERTY_RELAX_HOSTNAME, "true");
        if (StringUtil.equals(provider, CloudConstants.FILESYSTEM_SCHME)) {
            String path = System.getProperty("user.home") + "/.local/filesystemstorage";
            File localStorage = new File(path);
            if (!localStorage.exists()) {
                if (!localStorage.mkdirs()) {
                    throw new VlIOException("Could not create local folder: " + path);
                }
            }
            props.setProperty(FilesystemConstants.PROPERTY_BASEDIR, path);
        }

        BlobStoreContext blobStoreContext = new BlobStoreContextFactory().createContext(provider, props);
        this.asyncBlobStore = blobStoreContext.getAsyncBlobStore();
    }

    @Override
    public VFile newFile(VRL fileVrl) throws VlException {
        return new CloudFile(this, fileVrl);
    }

    @Override
    public VDir newDir(VRL dirVrl) throws VlException {
        return new CloudDir(this, dirVrl);
    }

    @Override
    public VFSNode openLocation(VRL vrl) throws VlException {
        try {
            return getPath(vrl);
        } catch (InterruptedException e) {
            throw new VlException(e);
        } catch (ExecutionException e) {
            throw new VlException(e);
        }
    }

    private VFSNode getPath(VRL pathVrl) throws VlException, InterruptedException, ExecutionException {
        // logger.debugPrintf("getPath():%s\n", pathVrl);
        String path = pathVrl.getPath();
        // normalize and make path absolute
        path = resolvePath(path);
        path = removePrefix(path);
        if (path.startsWith("/~")) {
            String homePath = getDefaultHome();
            if (homePath != null) {
                path = path.replaceFirst("/~", homePath);
            } else {
                path = path.replaceFirst("/~", "/");
            }

            pathVrl = pathVrl.copyWithNewPath(path);

            logger.debugPrintf("vrl: %s -> newPath %s\n", pathVrl,
                    pathVrl.copyWithNewPath(path));
        }
        VRL v = new VRL(path);
        // logger.debugPrintf("vrl queryPath():%s\n", v);

        // Maybe it's the root
        if (StringUtil.isEmpty(path) || StringUtil.equals(path, "/")) {
            return new CloudDir(this, pathVrl);
        }

        StorageMetadata meta = queryPath(v);
        if (meta == null) {
            return null;
        }
        return getResourceType(meta.getType(), pathVrl);
    }

    private String getDefaultHome() {
        return null;
    }

    private VFSNode getResourceType(StorageType storageType, VRL pathVrl) {
        switch (storageType) {
            case RELATIVE_PATH:
                return new CloudDir(this, pathVrl);
            case BLOB:
                return new CloudFile(this, pathVrl);
            case FOLDER:
                return new CloudDir(this, pathVrl);
            case CONTAINER:
                return new CloudDir(this, pathVrl);
            default:
                return new CloudNode(this, pathVrl);
        }
    }

    private String removePrefix(String path) {
        if (!path.contains(prefixPath)) {
            return path;
        }
        if (path.length() <= prefixPath.length()) {
            return "";
        }
        return path.substring(prefixPath.length());
    }

    protected StorageMetadata queryPath(VRL path) throws VRLSyntaxException, InterruptedException, ExecutionException, ResourceNotFoundException, CloudRequestTimeout {
        logger.debugPrintf("queryPath():%s\n", path);
        try {

            logger.debugPrintf("list: %s\n", path);

            String[] containerAndPath = getContainerAndPath(path);

            String container = containerAndPath[0];

            String restOfThePath = containerAndPath[1];

            // Check if we stat only the container
            if (StringUtil.isEmpty(restOfThePath)) {

                ListenableFuture<Boolean> res = asyncBlobStore.containerExists(container);

                block(res);

                if (res.get()) {
                    return new StorageMetadataImpl(StorageType.CONTAINER, null,
                            container, null, null, null, null,
                            new HashMap<String, String>());
                }

            } else {
                ListenableFuture<BlobMetadata> res = asyncBlobStore.blobMetadata(
                        container, restOfThePath);
                block(res);
                return res.get();
            }

        } catch (org.jclouds.blobstore.ContainerNotFoundException e) {
            throw new nl.uva.vlet.exception.ResourceNotFoundException(path
                    + " not found. " + e.getMessage());
        } finally {
            //blobContext.close();
        }
        return null;
    }

    private String[] getContainerAndPath(VRL path) throws VRLSyntaxException {
        String cleanPath = removePrefix(path.getPath());
        String[] pathElements = new VRL(cleanPath).getPathElements();
        String container = pathElements[0];
        // logger.debugPrintf("container: %s\n", container);
        String restOfThePath = null;
        if (pathElements.length > 1) {
            restOfThePath = cleanPath.toString().substring(
                    container.length() + 2);
        }

        return new String[]{container, restOfThePath};
    }

    @Override
    public boolean isConnected() {
        if (asyncBlobStore == null) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void connect() throws VlException {
    }

    @Override
    public void disconnect() throws VlException {
//        getBlobStoreContext().close();
        asyncBlobStore.getContext().close();
        asyncBlobStore = null;
    }

    @Override
    public void dispose() {
//        getBlobStoreContext().close();
        asyncBlobStore.getContext().close();
        asyncBlobStore = null;
    }

    public long getNumOfNodes(VRL vrl) throws VRLSyntaxException,
            InterruptedException, ExecutionException {
        //BlobStoreContext blobContext = getBlobStoreContext();
        String[] containerAndPath = getContainerAndPath(vrl);

        try {
            ListenableFuture<Long> res = null;
            if (containerAndPath.length <= 1
                    || StringUtil.isEmpty(containerAndPath[1])) {
                res = asyncBlobStore.countBlobs(containerAndPath[0]);
            } else {
                res = asyncBlobStore.countBlobs(
                        containerAndPath[0],
                        Builder.inDirectory(containerAndPath[1]));
            }

            return res.get();

        } finally {
            //blobContext.close();
        }
    }

    protected VDir mkdir(VRL vrl, boolean ignoreExisting) throws VRLSyntaxException, InterruptedException, ExecutionException, ResourceAlreadyExistsException, ResourceCreationFailedException, ResourceException {
        String[] containerAndPath = getContainerAndPath(vrl);
        //Create container
        if (containerAndPath.length <= 1
                || StringUtil.isEmpty(containerAndPath[1])) {
            createContainer(containerAndPath[0], ignoreExisting);
        } else {
            createFolder(containerAndPath, ignoreExisting);
        }
        return new CloudDir(this, vrl);
    }

    private void block(ListenableFuture<?> res) throws InterruptedException,
            CloudRequestTimeout {
        int waitTime = 1;
        // For some reason 'get' alone causes interrupt exceptions
        while (!res.isDone()) {
            // logger.debugPrintf("containerExists done %s. Next poll %s\n",
            // resContainerExists.isDone(),waitTime);
            Thread.sleep(waitTime);
            waitTime = waitTime * 2;

            if (waitTime > CloudConstants.TIME_OUT) {
                throw new CloudRequestTimeout();
            }
        }
    }

    protected boolean exists(VRL vrl, StorageType type) throws VRLSyntaxException, InterruptedException, ExecutionException, ResourceException {
        if (vrl.isRootPath()) {
            return true;
        }
        String[] containerAndPath = getContainerAndPath(vrl);

        if (containerAndPath.length <= 1
                || StringUtil.isEmpty(containerAndPath[1])) {

            ListenableFuture<Boolean> resContainerExists = asyncBlobStore.containerExists(containerAndPath[0]);

            // This waits for request to complete
            return resContainerExists.get();

        } else if (containerAndPath.length > 1) {
            BlobMetadata meta = null;
            try {
                ListenableFuture<BlobMetadata> metaRes = asyncBlobStore.blobMetadata(containerAndPath[0], containerAndPath[1]);
                meta = metaRes.get();
            } catch (Exception ex) {
                if (ex.getMessage().contains("(Is a directory)")) {
                    if (type != StorageType.BLOB) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    throw new ResourceException(ex.getMessage());
                }
            }

            if (meta != null) {
                if (type == meta.getType()) {
                    return true;
                }
                //We can have FOLDER or RELATIVE_PATH so it's easier to rule out it's not BLOB
                if (type != StorageType.BLOB && meta.getType() != StorageType.BLOB) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean rm(VRL vrl, StorageType type) throws VRLSyntaxException,
            InterruptedException, ExecutionException {
        String[] containerAndPath = getContainerAndPath(vrl);

        if (containerAndPath.length <= 1
                || StringUtil.isEmpty(containerAndPath[1])) {

            ListenableFuture<Void> res = asyncBlobStore.deleteContainer(containerAndPath[0]);
            // This waits for request to complete
            res.get();
            return true;
        } else if (containerAndPath.length > 1) {
            ListenableFuture<Void> resRemove = null;
            if (type != StorageType.BLOB) {
                resRemove = asyncBlobStore.deleteDirectory(containerAndPath[0], containerAndPath[1]);
            } else {
                resRemove = asyncBlobStore.removeBlob(containerAndPath[0], containerAndPath[1]);
            }

            resRemove.get();
            return true;
        }
        return false;
    }

    public VFSNode[] ls(VRL vrl) throws VRLSyntaxException,
            InterruptedException, ExecutionException {
        //BlobStoreContext blobContext = getBlobStoreContext();
        VFSNode[] nodesArry = null;
        try {
            String[] containerAndPath = getContainerAndPath(vrl);

            ListenableFuture<PageSet<? extends StorageMetadata>> result;

//            AsyncBlobStore blobStore = getAsyncBlobStore(); //AsyncBlobStore blobStore = blobContext.getAsyncBlobStore();

            if (containerAndPath.length <= 1
                    || StringUtil.isEmpty(containerAndPath[1])) {

                logger.debugPrintf(">>>list(%s).\n ", containerAndPath[0]);

                result = asyncBlobStore.list(containerAndPath[0]);

                // while (result.isDone()) {
                // logger.debugPrintf(">>>ls(%s) not done.\n ", vrl);
                // }

            } else {
                logger.debugPrintf(">>>list(%s/%s).\n ", containerAndPath[0],
                        containerAndPath[1]);
                result = asyncBlobStore.list(containerAndPath[0],
                        Builder.inDirectory(containerAndPath[1]));

                // while (result.isDone()) {
                // logger.debugPrintf(">>>ls(%s) not done.\n ", vrl);
                // }

            }

            ArrayList<VFSNode> nodes = new ArrayList<VFSNode>();

            PageSet<? extends StorageMetadata> list = result.get();

            for (StorageMetadata s : list) {
                VRL nodeVRL = vrl.append(s.getName());
                VFSNode node = getResourceType(s.getType(), nodeVRL);
                nodes.add(node);
            }

            nodesArry = new VFSNode[nodes.size()];
            nodesArry = nodes.toArray(nodesArry);
        } finally {
            //blobContext.close();
        }
        return nodesArry;
    }

    protected VRL mv(VRL vrl, String newNameOrPath, boolean nameIsPath)
            throws VlException {

        //BlobStoreContext blobContext = getBlobStoreContext();
        throw new nl.uva.vlet.exception.NotImplementedException(
                "Cloud storage services don't support move or rename!");

    }

    public InputStream getInputStream(VRL vrl) throws VRLSyntaxException,
            InterruptedException, ExecutionException {
        //BlobStoreContext blobContext = getBlobStoreContext();
        try {

            String[] containerAndPath = getContainerAndPath(vrl);

//            AsyncBlobStore blobStore = getAsyncBlobStore(); //AsyncBlobStore blobStore = blobContext.getAsyncBlobStore();

            ListenableFuture<Blob> res = asyncBlobStore.getBlob(containerAndPath[0],
                    containerAndPath[1]);

            Blob blob = res.get();

            return blob.getPayload().getInput();

        } finally {
            //blobContext.close();
        }
    }

    public OutputStream getOutputStream(VRL vrl) throws VRLSyntaxException, IOException {

        String[] containerAndPath = getContainerAndPath(vrl);
//        return new CloudOutputStream(containerAndPath[0], containerAndPath[1],
//                provider, props);
        return new CloudOutputStream(containerAndPath[0], containerAndPath[1], asyncBlobStore);
    }

    public boolean touch(VRL vrl, boolean ignoreExisting) throws VRLSyntaxException, InterruptedException, ExecutionException, ResourceAlreadyExistsException {
        String[] containerAndPath = getContainerAndPath(vrl);
        //Exists ?
        ListenableFuture<BlobMetadata> blobMeta = null;
        BlobMetadata meta = null;
        try {
            blobMeta = asyncBlobStore.blobMetadata(containerAndPath[0], containerAndPath[1]);
            meta = blobMeta.get();
        } catch (Exception ex) {
            if (ex.getMessage().contains("(Is a directory)")) {
                throw new ResourceAlreadyExistsException(
                        vrl + " already exists as a folder");
            }
        }

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
            throw new ResourceAlreadyExistsException(
                    vrl + " already exists as a folder");
        } else if (meta.getType() == StorageType.BLOB && !ignoreExisting) {
            throw new ResourceAlreadyExistsException(
                    vrl + " already exists");
        } else if (meta.getType() == StorageType.BLOB && ignoreExisting) {
            return true;
        }
        return false;
    }

    private void debug(String msg) {
//        System.err.println(this.getClass().getName() + ": " + msg);
//        System.out.println(this.getClass().getName() + ": " + msg);
        logger.debugPrintf(msg + "\n");
//        Global.debugPrintf(this, msg + "\n");
    }

    private void createContainer(String container, boolean ignoreExisting) throws InterruptedException, ExecutionException, ResourceAlreadyExistsException, ResourceCreationFailedException {
        ListenableFuture<Boolean> res = asyncBlobStore.containerExists(container);
        Boolean containerExists = res.get();
        if (containerExists && !ignoreExisting) {
            throw new ResourceAlreadyExistsException(
                    container + "Exists");
        } else if (containerExists) {
            //return new CloudDir(this, vrl);
            return;
        }
        ListenableFuture<Boolean> createContainer = asyncBlobStore.createContainerInLocation(null,
                container);
        Boolean created = createContainer.get();

        if (!created) {
            throw new ResourceCreationFailedException(
                    "Could not create " + container);
        }
        //return new CloudDir(this, vrl);
        return;
    }

    private void createFolder(String[] containerAndPath, boolean ignoreExisting) throws InterruptedException, ExecutionException, ResourceAlreadyExistsException, ResourceException {
        //Exists ?
        BlobMetadata meta = null;
        try {
            ListenableFuture<BlobMetadata> blobMeta = asyncBlobStore.blobMetadata(containerAndPath[0], containerAndPath[1]);
            meta = blobMeta.get();
        } catch (Exception ex) {
            if (ex.getMessage().contains("(Is a directory)") && !ignoreExisting) {
                throw new ResourceAlreadyExistsException(ex.getMessage());
            } else {
                return;
            }
        }
        if (meta == null) {
            //Ok non existing
//            try {
            ListenableFuture<Void> createdDirRes = asyncBlobStore.createDirectory(containerAndPath[0], containerAndPath[1]);
            createdDirRes.get();
            //return new CloudDir(this, vrl);
            return;
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
