package nl.uva.vlet.vfs.cloud;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.*;
import java.util.concurrent.ExecutionException;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.PutOptions;

public class CloudOutputStream extends OutputStream {

//    private String provider;
//    private Properties props;
    private String container;
    private String blobName;
//    private BlobStoreContext blobContext;
    private File bufferFile;
    private OutputStream out;
    private final AsyncBlobStore asyncBlobStore;
    private int bytesWriten = 0;
    private final ListenableFuture<Blob> res;

    CloudOutputStream(String container, String blobName, AsyncBlobStore asyncBlobStore) throws IOException {
        this.container = container;
        this.blobName = blobName;

        this.asyncBlobStore = asyncBlobStore;
        out = new ByteArrayOutputStream();

        //Get blob asynchronously
        res = asyncBlobStore.getBlob(container, blobName);
    }

    @Override
    public void write(final int b) throws IOException {
        bytesWriten++;
        if (bytesWriten < Constants.OUTPUT_STREAM_BUFFER_SIZE_IN_BYTES) {
            out.write(b);
        } else {
            dumpTheArrayAndSwitchToFile();
            out.write(b);
        }
    }

    private void writeData() throws InterruptedException, ExecutionException {
        try {
            //Get blob asynchronously
            Blob blob = res.get();
//            Multimap<String, String> head = null;
//            blob.setAllHeaders(head);
            if (blob == null) {
                blob = asyncBlobStore.blobBuilder(blobName).build();
            }
            if (out instanceof ByteArrayOutputStream) {
                blob.setPayload(((ByteArrayOutputStream) out).toByteArray());
                asyncBlobStore.getContext().getBlobStore().putBlob(container, blob);
            } else if (out instanceof FileOutputStream) {
                blob.setPayload(bufferFile);
                if (bufferFile.length() > (50 * 1024 * 1024)) {
                    asyncBlobStore.getContext().getBlobStore().putBlob(container, blob, PutOptions.Builder.multipart());
                } else {
                    asyncBlobStore.getContext().getBlobStore().putBlob(container, blob);
                }
            }
        } finally {
            if (bufferFile != null) {
                bufferFile.delete();
            }
        }
    }

    @Override
    public void write(final byte[] b, final int off, final int len)
            throws IOException {
        bytesWriten += len;
        if (bytesWriten < Constants.OUTPUT_STREAM_BUFFER_SIZE_IN_BYTES) {
            out.write(b, off, len);
        } else {
            dumpTheArrayAndSwitchToFile();
            out.write(b, off, len);
        }
    }

    @Override
    public void write(final byte[] b) throws IOException {
        bytesWriten += b.length;
        if (bytesWriten < Constants.OUTPUT_STREAM_BUFFER_SIZE_IN_BYTES) {
            out.write(b);
        } else {
            dumpTheArrayAndSwitchToFile();
            out.write(b);
        }
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
        try {
            writeData();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
//        blobContext.close();
    }

    private void dumpTheArrayAndSwitchToFile() throws FileNotFoundException, IOException {
        if (bytesWriten < Constants.OUTPUT_STREAM_BUFFER_SIZE_IN_BYTES) {
            bufferFile = File.createTempFile(this.getClass().getSimpleName(), null);
            FileOutputStream fos = new FileOutputStream(bufferFile);
            bufferFile.deleteOnExit();

            fos.write(((ByteArrayOutputStream) out).toByteArray());
            out.flush();
            out.close();

            out = fos;
        }
    }
}