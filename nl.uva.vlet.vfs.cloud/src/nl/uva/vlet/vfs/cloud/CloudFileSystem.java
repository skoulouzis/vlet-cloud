package nl.uva.vlet.vfs.cloud;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nl.uva.vlet.ClassLogger;
import nl.uva.vlet.data.StringUtil;
import nl.uva.vlet.exception.*;
import nl.uva.vlet.vfs.*;
import nl.uva.vlet.vfs.cloud.Exceptions.CloudRequestTimeout;
import nl.uva.vlet.vrl.VRL;
import nl.uva.vlet.vrs.ServerInfo;
import nl.uva.vlet.vrs.VRSContext;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.*;
import org.jclouds.blobstore.domain.internal.StorageMetadataImpl;
import org.jclouds.blobstore.options.ListContainerOptions.Builder;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.http.HttpResponseException;
import org.jclouds.io.Payload;
import org.jclouds.openstack.keystone.v2_0.config.CredentialTypes;
import org.jclouds.openstack.keystone.v2_0.config.KeystoneProperties;

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
    private String[] prefixPaths = {"/auth/v1.0", "/v2.0/"};
    static ClassLogger logger;
    private HashMap<VRL, CloudMetadataWrapper> cache = new HashMap<VRL, CloudMetadataWrapper>();
    private Boolean doChunkUpload;
    private BlobStore blobstore;
    private int swiftVersion;
    private String endpoint;

    {
        try {
            logger = ClassLogger.getLogger(CloudFileSystem.class);
//            logger.setLevelToDebug();
            logger.setLevel(Level.OFF);
        } catch (SecurityException ex) {
            Logger.getLogger(CloudFileSystem.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
//    private AsyncBlobStore blobstore;
//    private boolean useCache = true;
    private boolean debug = true;
//    private static int bufferSize = -1;

    public CloudFileSystem(VRSContext context, ServerInfo info)
            throws VlInitializationException, VlPasswordException, VRLSyntaxException, VlIOException, VlException {
        super(context, info);

        init(info, context, "https");
        try {
            connect();
        } catch (Exception ex) {
            if (ex instanceof VlConfigurationError && ex.getMessage().contains("Unrecognized SSL message, plaintext connection?")) {
                disconnect();
                init(info, context, "http");
                connect();
            }
            if (ex.getMessage() != null && ex.getMessage().contains("Unrecognized SSL message, plaintext connection?")) {
            } else {
                throw new nl.uva.vlet.exception.VlInitializationException(ex.getMessage());
            }
        }
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
        VFSNode node;
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
            node = new CloudDir(this, pathVrl);
        } else {
            StorageMetadata meta = queryPath(v);
            if (meta == null) {
                node = null;
            } else {
                getCache().put(pathVrl, new CloudMetadataWrapper(meta));
                node = getResourceType(meta.getType(), pathVrl);
            }
        }
        return node;
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
        for (String p : prefixPaths) {
            if (path.length() <= p.length()) {
                return "";
            }
            if (path.contains(p)) {
                return path.substring(p.length());
            }
        }
        return path;
    }

    protected StorageMetadata queryPath(VRL path) throws VRLSyntaxException, InterruptedException, ExecutionException, ResourceNotFoundException, CloudRequestTimeout, VlException {
        logger.debugPrintf("queryPath():%s\n", path);
        StorageMetadata meta = null;
        try {
            logger.debugPrintf("list: %s\n", path);

            String[] containerAndPath = getContainerAndPath(path);

            String container = containerAndPath[0];

            String restOfThePath = containerAndPath[1];

            // Check if we stat only the container
            if (StringUtil.isEmpty(restOfThePath)) {
                //                ListenableFuture<Boolean> res = blobstore.containerExists(container);
                //                block(res);
                boolean exists = blobstore.containerExists(container);
                if (exists) {
                    meta = new StorageMetadataImpl(StorageType.CONTAINER, container, null, null, null, null, null, null, new HashMap<String, String>());
//                    meta = new StorageMetadataImpl(StorageType.CONTAINER, null,
//                            container, null, null, null, null,
//                            new HashMap<String, String>());
                }
            } else {
                if (blobstore == null) {
                    connect();
                }
                meta = blobstore.blobMetadata(
                        container, restOfThePath);
            }
        } catch (org.jclouds.blobstore.ContainerNotFoundException e) {
            throw new nl.uva.vlet.exception.ResourceNotFoundException(path
                    + " not found. " + e.getMessage());
        } finally {
            //blobContext.close();
        }
        getCache().put(path, new CloudMetadataWrapper(meta));
        return meta;
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
        if (blobstore == null) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void connect() throws VlException {
        BlobStoreContext blobStoreContext = ContextBuilder.newBuilder(provider).overrides(props).build(BlobStoreContext.class);
        blobstore = blobStoreContext.getBlobStore();
        try {
            blobstore.containerExists("");
        } catch (Exception ex) {
            if (ex instanceof org.jclouds.rest.AuthorizationException) {
                throw new nl.uva.vlet.exception.VlAuthenticationException(ex.getMessage());
            }
            if (ex instanceof HttpResponseException && ex.getMessage().contains("Unrecognized SSL message, plaintext connection?")) {
                throw new nl.uva.vlet.exception.VlConfigurationError(ex.getMessage());
            }
        }
        //        blobstore.countBlobs("");
        //        BlobStoreContext blobStoreContext = new BlobStoreContextFactory().createContext(provider, props);
        //        this.blobstore = blobStoreContext.getAsyncBlobStore();
    }

    @Override
    public void disconnect() throws VlException {
//        getBlobStoreContext().close();
        blobstore.getContext().close();
        blobstore = null;
        getCache().clear();
    }

    @Override
    public void dispose() {
        blobstore.getContext().close();
        blobstore = null;
        getCache().clear();
    }

    public long getNumOfNodes(VRL vrl) throws VRLSyntaxException,
            InterruptedException, ExecutionException, CloudRequestTimeout {
        long numOfNodes;
        String[] containerAndPath = getContainerAndPath(vrl);
        try {
            if (containerAndPath.length <= 1
                    || StringUtil.isEmpty(containerAndPath[1])) {
                numOfNodes = blobstore.countBlobs(containerAndPath[0]);
            } else {
                numOfNodes = blobstore.countBlobs(
                        containerAndPath[0],
                        Builder.inDirectory(containerAndPath[1]));
            }
            CloudMetadataWrapper cmw = getCache().get(vrl);
            if (cmw == null) {
                cmw = new CloudMetadataWrapper(null);
            }
            cmw.setNumOfNodes(numOfNodes);
            getCache().put(vrl, cmw);
            return numOfNodes;

        } finally {
            //blobContext.close();
        }
    }

    protected VDir mkdir(VRL vrl, boolean ignoreExisting) throws VRLSyntaxException, InterruptedException, ExecutionException, ResourceAlreadyExistsException, ResourceCreationFailedException, ResourceException, CloudRequestTimeout {
        String[] containerAndPath = getContainerAndPath(vrl);
        //Create container
        if (containerAndPath.length <= 1
                || StringUtil.isEmpty(containerAndPath[1])) {
            createContainer(containerAndPath[0], ignoreExisting);
        } else {
            createFolder(containerAndPath, ignoreExisting);
        }
        getCache().put(vrl, new CloudMetadataWrapper(null));
        return new CloudDir(this, vrl);
    }

    private void block(ListenableFuture<?> res) throws InterruptedException,
            CloudRequestTimeout {
        if (res != null) {
            int waitTime = 1;
            // For some reason 'get' alone causes interrupt exceptions
            while (!res.isDone()) {
                // logger.debugPrintf("containerExists done %s. Next poll %s\n",
                // resContainerExists.isDone(),waitTime);
                Thread.sleep(waitTime);
                waitTime = waitTime * 2;

                if (waitTime > Constants.TIME_OUT) {
                    throw new CloudRequestTimeout();
                }
            }
        }

    }

    protected boolean exists(VRL vrl, StorageType type) throws VRLSyntaxException, InterruptedException, ExecutionException, ResourceException, CloudRequestTimeout, VlException {
        boolean exists = false;
        BlobMetadata meta = null;
        if (vrl.isRootPath()) {
            exists = true;
        } else {
            String[] containerAndPath = getContainerAndPath(vrl);

            if (containerAndPath.length <= 1
                    || StringUtil.isEmpty(containerAndPath[1])) {
                if (blobstore == null) {
                    debug("blobstore == null");
                    this.connect();
                }
                exists = blobstore.containerExists(containerAndPath[0]);
            } else if (containerAndPath.length > 1) {
                try {
                    meta = blobstore.blobMetadata(containerAndPath[0], containerAndPath[1]);
                } catch (Exception ex) {
                    if (ex != null && ex.getMessage() != null && ex.getMessage().contains("(Is a directory)")) {
                        if (type != StorageType.BLOB) {
                            exists = true;
                        } else {
                            exists = false;
                        }
                    } else {
                        throw new ResourceException(ex.getMessage());
                    }
                }

                if (meta != null) {
                    if (type == meta.getType()) {
                        exists = true;
                    }
                    //We can have FOLDER or RELATIVE_PATH so it's easier to rule out it's not BLOB
                    if (type != StorageType.BLOB && meta.getType() != StorageType.BLOB) {
                        exists = true;
                    }
                }
            }
        }
        CloudMetadataWrapper cwm = getCache().get(vrl);
        if (exists && cwm == null) {
            getCache().put(vrl, new CloudMetadataWrapper(meta));
        }
        return exists;
    }

    public boolean rm(VRL vrl, StorageType type) throws VRLSyntaxException,
            InterruptedException, ExecutionException, CloudRequestTimeout {
        boolean removed = false;
        String[] containerAndPath = getContainerAndPath(vrl);

        if (containerAndPath.length <= 1
                || StringUtil.isEmpty(containerAndPath[1])) {

            blobstore.deleteContainer(containerAndPath[0]);
            removed = true;
        } else if (containerAndPath.length > 1) {

            if (type != StorageType.BLOB) {
                blobstore.deleteDirectory(containerAndPath[0], containerAndPath[1]);
            } else {
                PageSet<? extends StorageMetadata> list = blobstore.list(containerAndPath[0],
                        Builder.inDirectory(containerAndPath[1]));

                for (StorageMetadata m : list) {
                    blobstore.removeBlob(containerAndPath[0], m.getName());
                }
                blobstore.removeBlob(containerAndPath[0], containerAndPath[1]);
            }
            removed = true;
        }
        if (removed) {
            getCache().remove(vrl);
        }
        return removed;
    }

    public VFSNode[] ls(VRL vrl) throws VRLSyntaxException,
            InterruptedException, ExecutionException, CloudRequestTimeout {
        //BlobStoreContext blobContext = getBlobStoreContext();
        VFSNode[] nodesArry = null;
        try {
            String[] containerAndPath = getContainerAndPath(vrl);


            PageSet<? extends StorageMetadata> list;
            if (containerAndPath.length <= 1
                    || StringUtil.isEmpty(containerAndPath[1])) {

                logger.debugPrintf("list(%s).\n ", containerAndPath[0]);

                list = blobstore.list(containerAndPath[0]);

            } else {
                logger.debugPrintf("list(%s/%s).\n ", containerAndPath[0],
                        containerAndPath[1]);
                list = blobstore.list(containerAndPath[0],
                        Builder.inDirectory(containerAndPath[1]));

            }

            ArrayList<VFSNode> nodes = new ArrayList<VFSNode>();

            for (StorageMetadata s : list) {
                VRL nodeVRL = vrl.append(s.getName());
                VFSNode node = getResourceType(s.getType(), nodeVRL);
                nodes.add(node);
                getCache().put(nodeVRL, new CloudMetadataWrapper(s));
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
            InterruptedException, ExecutionException, CloudRequestTimeout {
        try {

            String[] containerAndPath = getContainerAndPath(vrl);

            Blob blob = blobstore.getBlob(containerAndPath[0],
                    containerAndPath[1]);

            Payload payload = blob.getPayload();
            return new BufferedInputStream(payload.getInput(), 5242880);

        } finally {
            //blobContext.close();
        }
    }

    public OutputStream getOutputStream(VRL vrl) throws VRLSyntaxException, IOException, InterruptedException, ExecutionException {

        String[] containerAndPath = getContainerAndPath(vrl);
//        return new CloudOutputStream(containerAndPath[0], containerAndPath[1],
//                provider, props);
//        if (vrl.getScheme().equals("swift")) {
//        if (vrl.getScheme().equals("swift") && doChunkUpload != null && doChunkUpload) {
//            return new SwiftCloudOutputStream(containerAndPath[0], containerAndPath[1], blobstore, props.getProperty(org.jclouds.Constants.PROPERTY_CREDENTIAL));
//        } else {
        return new CloudOutputStream(containerAndPath[0], containerAndPath[1], blobstore);
//        }

    }

    public boolean touch(VRL vrl, boolean ignoreExisting) throws VRLSyntaxException, InterruptedException, ExecutionException, ResourceAlreadyExistsException, CloudRequestTimeout {
        boolean created = false;
        String[] containerAndPath = getContainerAndPath(vrl);
        //Exists ?
        ListenableFuture<BlobMetadata> blobMeta = null;
        BlobMetadata meta = null;
        try {
            meta = blobstore.blobMetadata(containerAndPath[0], containerAndPath[1]);
        } catch (Exception ex) {
            if (ex.getMessage().contains("(Is a directory)")) {
                throw new ResourceAlreadyExistsException(
                        vrl + " already exists as a folder");
            }
        }

        if (meta == null) {
            //Ok non existing 
            Blob blob = blobstore.blobBuilder(containerAndPath[1]).type(StorageType.BLOB).build();
            blob.setPayload("");
            String tag = blobstore.putBlob(containerAndPath[0], blob);
            created = true;
        } else {
            //What is it?
            if (meta.getType() != StorageType.BLOB) {
                throw new ResourceAlreadyExistsException(
                        vrl + " already exists as a folder");
            } else if (meta.getType() == StorageType.BLOB && !ignoreExisting) {
                throw new ResourceAlreadyExistsException(
                        vrl + " already exists");
            } else if (meta.getType() == StorageType.BLOB && ignoreExisting) {
                created = true;
            }
        }
        getCache().put(vrl, new CloudMetadataWrapper(meta));
        return created;
    }

    private void debug(String msg) {
        if (debug) {
            System.err.println(this.getClass().getName() + ": " + msg);
        }
//        System.out.println(this.getClass().getName() + ": " + msg);
//        logger.debugPrintf(msg + "\n");
//        Global.debugPrintf(this, msg + "\n");
    }

    private boolean createContainer(String container, boolean ignoreExisting) throws InterruptedException, ExecutionException, ResourceAlreadyExistsException, ResourceCreationFailedException, CloudRequestTimeout {
        Boolean containerExists = blobstore.containerExists(container);
        if (containerExists && !ignoreExisting) {
            throw new ResourceAlreadyExistsException(
                    container + "Exists");
        } else if (containerExists) {
            //return new CloudDir(this, vrl);
            return true;
        }
        Boolean created = blobstore.createContainerInLocation(null,
                container);
        if (!created) {
            throw new ResourceCreationFailedException(
                    "Could not create " + container);
        }
        //return new CloudDir(this, vrl);        
        return true;
    }

    private void createFolder(String[] containerAndPath, boolean ignoreExisting) throws InterruptedException, ExecutionException, ResourceAlreadyExistsException, ResourceException, CloudRequestTimeout {
        //Exists ?
        BlobMetadata meta = null;
        try {
            meta = blobstore.blobMetadata(containerAndPath[0], containerAndPath[1]);
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
            blobstore.createDirectory(containerAndPath[0], containerAndPath[1]);
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

    void setContentsToFile(byte[] bytes, VRL vrl) throws VRLSyntaxException, VlException {
        String[] containerAndPath = getContainerAndPath(vrl);
        Blob blob = blobstore.getBlob(containerAndPath[0], containerAndPath[1]);
        if (blob == null) {
            blob = blobstore.blobBuilder(containerAndPath[1]).build();
        }
        blob.setPayload(bytes);
        blobstore.getContext().getBlobStore().putBlob(containerAndPath[0], blob);
    }

    void setContentsToFile(String contents, VRL vrl) throws VRLSyntaxException, VlException {
        String[] containerAndPath = getContainerAndPath(vrl);
        Blob blob = blobstore.getBlob(containerAndPath[0], containerAndPath[1]);
        if (blob == null) {
            blob = blobstore.blobBuilder(containerAndPath[1]).build();
        }
        blob.setPayload(contents);
        blobstore.getContext().getBlobStore().putBlob(containerAndPath[0], blob);
    }

    /**
     * @return the cache
     */
    private HashMap<VRL, CloudMetadataWrapper> getCache() {
//        if (useCache) {
        return cache;
//        } else {
//            return null;
//        }
    }

    void uploadFile(VFSTransfer transferInfo, VFile localSource, VRL vrl) throws VRLSyntaxException, InterruptedException, CloudRequestTimeout, ExecutionException, VlException, IOException {
        if (transferInfo != null) {
            transferInfo.startSubTask("UploadToSwift", localSource.getLength());
        }
        String[] containerAndPath = getContainerAndPath(vrl);
        Blob blob = blobstore.getBlob(containerAndPath[0], containerAndPath[1]);
        if (blob == null) {
            blob = blobstore.blobBuilder(containerAndPath[1]).build();
        }
        File file = new File(localSource.getVRL().toURI());
        Long fileLimit = Long.valueOf("2147483648");
        if (file.length() > fileLimit && vrl.getScheme().equals("swift") && swiftVersion == 1) {
            ChunkUploader uploader = new ChunkUploader(file, containerAndPath[0], containerAndPath[1], blobstore, props.getProperty(org.jclouds.Constants.PROPERTY_CREDENTIAL), props.getProperty(org.jclouds.Constants.PROPERTY_IDENTITY), endpoint);
            uploader.upload();
        } else {
            blob.setPayload(file);
            if (file.length() > (50 * 1024 * 1024)) {
                blobstore.getContext().getBlobStore().putBlob(containerAndPath[0], blob, PutOptions.Builder.multipart());
            } else {
                blobstore.getContext().getBlobStore().putBlob(containerAndPath[0], blob);
            }
        }
        if (transferInfo != null) {
            transferInfo.endTask("UploadToSwift");
        }
    }

    private void init(ServerInfo info, VRSContext context, String authServiceSchema) throws VlIOException, VlPasswordException, VlInitializationException {
        provider = info.getScheme();// "swift";// "in-memory"
//        VAttribute attr = info.getAttribute("chunk.upload");
//        if (attr != null) {
//            doChunkUpload = attr.getBooleanValue();
//        } else {
//            doChunkUpload = false;
//        }
        doChunkUpload = (Boolean) getContext().getProperty("chunk.upload");
        props = new Properties();

        if (StringUtil.isEmpty(provider)) {
            throw new NullPointerException("Provider is null!");
        }
        swiftVersion = 1;
        VRL serverVRL = info.getServerVRL();
        if (!serverVRL.getScheme().equals(Constants.FILESYSTEM_SCHME)) {
            //get rid of all paths 
            VRL tmp = serverVRL.copyWithNewPath("");
            VRL tmp2 = tmp.appendPath(serverVRL.getPathElements()[0]);
            VRL newServerVRL;
            if (serverVRL.getPathElements()[0].equals("auth")) {
                newServerVRL = tmp2.appendPath(serverVRL.getPathElements()[1]);
            } else {
                provider = "swift-keystone";
                props.setProperty(KeystoneProperties.CREDENTIAL_TYPE, CredentialTypes.PASSWORD_CREDENTIALS);
                props.setProperty(org.jclouds.Constants.PROPERTY_API_VERSION, "2");
                newServerVRL = tmp2;
                swiftVersion = 2;
            }

            endpoint = newServerVRL.copyWithNewScheme(authServiceSchema).toString();
//            debug("Endpoint: " + endpoint);

            if (StringUtil.isEmpty(endpoint)) {
                throw new nl.uva.vlet.exception.VlInitializationException(
                        "Cloud service endpoint is null");
            }

            props.setProperty(org.jclouds.Constants.PROPERTY_ENDPOINT, endpoint);
            info.setAuthScheme(ServerInfo.PASSWORD_OR_PASSPHRASE_AUTH);
        }

        String username = info.getUsername();
        if (StringUtil.isEmpty(username)) {
            ServerInfo[] moreInfo = context.getServerInfoRegistry().getServerInfosFor(serverVRL);
            for (ServerInfo i : moreInfo) {
                username = i.getUsername();
                if (!StringUtil.isEmpty(username)) {
                    break;
                }
            }
            if (StringUtil.isEmpty(username)) {
                throw new NullPointerException("Username is null!");
            }

//            if (bufferSize <= -1) {
//                OperatingSystemMXBean osMBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
//                bufferSize = (int) (osMBean.getFreePhysicalMemorySize() / 10);
//                debug("Alocated  physical memory:\t" + bufferSize / (1024.0 * 1024.0));
//            }

        }

//        debug("Username: " + username);
        String passwd = info.getPassword();
//        debug("Password: " + passwd);
        if (StringUtil.isEmpty(passwd)) {
            ServerInfo[] moreInfo = context.getServerInfoRegistry().getServerInfosFor(serverVRL);
            for (ServerInfo i : moreInfo) {
                passwd = i.getPassword();
                if (!StringUtil.isEmpty(passwd)) {
                    break;
                }
            }
            if (StringUtil.isEmpty(passwd)) {
                debug("password is empty!!!");
                throw new nl.uva.vlet.exception.VlPasswordException(
                        "Cloud service credential (password) is null");
            }
        }

        props.setProperty(org.jclouds.Constants.PROPERTY_IDENTITY, username);
        props.setProperty(org.jclouds.Constants.PROPERTY_CREDENTIAL, passwd);
        props.setProperty(org.jclouds.Constants.PROPERTY_TRUST_ALL_CERTS,
                "true");

        props.setProperty(org.jclouds.Constants.PROPERTY_RELAX_HOSTNAME, "true");
        if (StringUtil.equals(provider, Constants.FILESYSTEM_SCHME)) {
            String path = System.getProperty("user.home") + "/.local/filesystemstorage";
            File localStorage = new File(path);
            if (!localStorage.exists()) {
                if (!localStorage.mkdirs()) {
                    throw new VlIOException("Could not create local folder: " + path);
                }
            }
            props.setProperty(FilesystemConstants.PROPERTY_BASEDIR, path);
        }
    }
}
