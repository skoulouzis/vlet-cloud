package nl.uva.vlet.vfs.cloud;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nl.uva.vlet.data.StringUtil;
import nl.uva.vlet.exception.VRLSyntaxException;
import nl.uva.vlet.exception.VlException;
import nl.uva.vlet.vfs.VChecksum;
import nl.uva.vlet.vfs.VFile;
import nl.uva.vlet.vfs.VFileSystem;
import nl.uva.vlet.vrl.VRL;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;

/**
 *
 *
 * @author S. Koulouzis
 */
public class CloudFile extends VFile implements VChecksum {

    private CloudFileSystem cvfs;

    public CloudFile(VFileSystem vfs, VRL vrl) {
        super(vfs, vrl);
        this.cvfs = (CloudFileSystem) vfs;
    }

    @Override
    public InputStream getInputStream() throws VlException {
        try {
            return cvfs.getInputStream(getVRL());
        } catch (InterruptedException e) {
            throw new VlException(e);
        } catch (ExecutionException e) {
            throw new VlException(e);
        }
    }

    @Override
    public OutputStream getOutputStream() throws VlException, VRLSyntaxException {
        try {
            return cvfs.getOutputStream(getVRL());
        } catch (IOException e) {
            throw new nl.uva.vlet.exception.VlIOException(e);
        } catch (InterruptedException ex) {
            throw new nl.uva.vlet.exception.VlException(ex);
        } catch (ExecutionException ex) {
            throw new nl.uva.vlet.exception.VlException(ex);
        }
    }

    @Override
    public boolean delete() throws VlException {
        try {
            return cvfs.rm(getVRL(), StorageType.BLOB);
        } catch (InterruptedException e) {
            throw new VlException(e);
        } catch (ExecutionException e) {
            throw new VlException(e);
        }
    }

    @Override
    public boolean exists() throws VlException {
        try {
            return cvfs.exists(getVRL(), StorageType.BLOB);
        } catch (InterruptedException e) {
            throw new VlException(e);
        } catch (ExecutionException e) {
            throw new VlException(e);
        }
    }

    @Override
    public long getLength() throws VlException {
        BlobMetadata meta;
        try {
            meta = (BlobMetadata) cvfs.queryPath(getVRL());
            
            return meta.getContentMetadata().getContentLength();
        } catch (InterruptedException e) {
            throw new VlException(e);
        } catch (ExecutionException e) {
            throw new VlException(e);
        }
    }

    @Override
    public VRL rename(String newNameOrPath, boolean nameIsPath)
            throws VlException {
        throw new VlException("Rename not supported in jclouds!");
    }

    @Override
    public boolean create(boolean ignoreExisting) throws VlException {
        try {
            return cvfs.touch(getVRL(), ignoreExisting);
        } catch (InterruptedException e) {
            throw new VlException(e);
        } catch (ExecutionException e) {
            throw new VlException(e);
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

    @Override
    public String[] getChecksumTypes() throws VlException {
        return new String[]{VChecksum.MD5};
    }
    static final byte[] HEX_CHAR_TABLE = {(byte) '0', (byte) '1', (byte) '2',
        (byte) '3', (byte) '4', (byte) '5', (byte) '6', (byte) '7',
        (byte) '8', (byte) '9', (byte) 'a', (byte) 'b', (byte) 'c',
        (byte) 'd', (byte) 'e', (byte) 'f'};

    @Override
    public String getChecksum(String algorithm) throws VlException {
        if (StringUtil.equals(algorithm, VChecksum.MD5)) {
            BlobMetadata meta;
            try {
                meta = (BlobMetadata) cvfs.queryPath(getVRL());

                byte[] raw = meta.getContentMetadata().getContentMD5();
                byte[] hex = new byte[2 * raw.length];

                int index = 0;
                for (byte b : raw) {
                    int v = b & 0xFF;
                    hex[index++] = HEX_CHAR_TABLE[v >>> 4];
                    hex[index++] = HEX_CHAR_TABLE[v & 0xF];
                }

                return new String(hex, "ASCII");
            } catch (InterruptedException e) {
                throw new VlException(e);
            } catch (ExecutionException e) {
                throw new VlException(e);
            } catch (UnsupportedEncodingException e) {
                throw new VlException(e);
            }
        }
        return null;
    }

    @Override
    public void setContents(byte[] bytes) throws VlException {
        this.cvfs.setContentsToFile(bytes, getVRL());
    }

    @Override
    public void setContents(String contents) throws VlException {
        this.cvfs.setContentsToFile(contents, getVRL());
    }
}
