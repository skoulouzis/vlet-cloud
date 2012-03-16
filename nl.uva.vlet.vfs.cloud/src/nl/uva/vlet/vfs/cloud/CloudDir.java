package nl.uva.vlet.vfs.cloud;

import java.util.Date;
import java.util.concurrent.ExecutionException;

import nl.uva.vlet.ClassLogger;
import nl.uva.vlet.exception.VlException;
import nl.uva.vlet.vfs.VDir;
import nl.uva.vlet.vfs.VFSNode;
import nl.uva.vlet.vrl.VRL;

import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;

/**
 * 
 * 
 * @author S. Koulouzis
 */
public class CloudDir extends VDir {// implements VUnixFileMode {

    private CloudFileSystem cvfs;
    static ClassLogger logger;

    static {
        logger = ClassLogger.getLogger(CloudDir.class);
        logger.setLevelToDebug();
    }

    public CloudDir(CloudFileSystem vfsSystem, VRL dirVrl) {
        super(vfsSystem, dirVrl);
        this.cvfs = vfsSystem;
    }

    @Override
    public long getNrOfNodes() throws VlException {
        try {
            return cvfs.getNumOfNodes(getVRL());
        } catch (InterruptedException e) {
            throw new VlException(e);
        } catch (ExecutionException e) {
            throw new VlException(e);
        }
    }

    @Override
    public boolean delete(boolean recurse) throws VlException {
        try {
            return cvfs.rm(getVRL(),StorageType.FOLDER);
        } catch (InterruptedException e) {
            throw new VlException(e);
        } catch (ExecutionException e) {
            throw new VlException(e);
        }
    }

    @Override
    public VFSNode[] list() throws VlException {
        try {
            return cvfs.ls(getVRL());
        } catch (InterruptedException e) {
            throw new VlException(e);
        } catch (ExecutionException e) {
            throw new VlException(e);
        }
    }

    @Override
    public VRL rename(String newNameOrPath, boolean nameIsPath)
            throws VlException {

        return cvfs.mv(getVRL(), newNameOrPath, nameIsPath);
    }

    @Override
    public boolean create(boolean ignoreExisting) throws VlException {
        VDir dir;
        try {
            dir = cvfs.mkdir(getVRL(), ignoreExisting);
            return (dir != null);
        } catch (InterruptedException e) {
            throw new VlException(e);
        } catch (ExecutionException e) {
            throw new VlException(e);
        }
    }

    @Override
    public boolean exists() throws VlException {
        VRL vrl = getVRL();
        try {
            return cvfs.exists(vrl, StorageType.FOLDER);
        } catch (Exception e) {
            if (e instanceof org.jclouds.blobstore.ContainerNotFoundException || e.getMessage().contains(org.jclouds.blobstore.ContainerNotFoundException.class.getName())  ) {
                return false;
            } else {
                throw new nl.uva.vlet.exception.VlException(e);
            }
        }
    }

    @Override
    public long getModificationTime() throws VlException {

        StorageMetadata meta;
        try {
            meta = cvfs.queryPath(getVRL());
            if (meta != null) {
                Date date = meta.getLastModified();
                if (date != null) {
                    return date.getTime();
                }
            }
            return 0;
        } catch (InterruptedException e) {
            throw new VlException(e);
        } catch (ExecutionException e) {
            throw new VlException(e);
        }

    }

    @Override
    public boolean isReadable() throws VlException {
        // TODO For now everything is r/w
        return true;
    }

    @Override
    public boolean isWritable() throws VlException {
        // TODO For now everything is r/w
        return true;
    }
}
