/**
 *
 */
package nl.uva.vlet.vfs.cloud;

import nl.uva.vlet.exception.VlException;
import nl.uva.vlet.vfs.VDir;
import nl.uva.vlet.vfs.VFSNode;
import nl.uva.vlet.vfs.VFileSystem;
import nl.uva.vlet.vrl.VRL;

/**
 * @author skoulouz
 *
 */
public class CloudNode extends VFSNode {

    /**
     * @param vfs
     * @param vrl
     */
    public CloudNode(VFileSystem vfs, VRL vrl) {
        super(vfs, vrl);
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vrs.VDeletable#delete()
     */
    @Override
    public boolean delete() throws VlException {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#rename(java.lang.String, boolean)
     */
    @Override
    public VRL rename(String newNameOrPath, boolean nameIsPath)
            throws VlException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#copyTo(nl.uva.vlet.vfs.VDir)
     */
    @Override
    public VFSNode copyTo(VDir dest) throws VlException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#copyTo(nl.uva.vlet.vfs.VDir,
     * java.lang.String)
     */
    @Override
    public VFSNode copyTo(VDir dest, String newName) throws VlException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#moveTo(nl.uva.vlet.vfs.VDir)
     */
    @Override
    public VFSNode moveTo(VDir dest) throws VlException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#moveTo(nl.uva.vlet.vfs.VDir,
     * java.lang.String)
     */
    @Override
    public VFSNode moveTo(VDir dest, String newName) throws VlException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#create(boolean)
     */
    @Override
    public boolean create(boolean ignoreExisting) throws VlException {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#isFile()
     */
    @Override
    public boolean isFile() {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#isDir()
     */
    @Override
    public boolean isDir() {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#exists()
     */
    @Override
    public boolean exists() throws VlException {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#getModificationTime()
     */
    @Override
    public long getModificationTime() throws VlException {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#isReadable()
     */
    @Override
    public boolean isReadable() throws VlException {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vfs.VFSNode#isWritable()
     */
    @Override
    public boolean isWritable() throws VlException {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see nl.uva.vlet.vrs.VNode#getType()
     */
    @Override
    public String getType() {
        // TODO Auto-generated method stub
        return null;
    }
}
