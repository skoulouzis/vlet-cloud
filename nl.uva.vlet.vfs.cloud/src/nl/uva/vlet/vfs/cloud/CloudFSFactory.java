package nl.uva.vlet.vfs.cloud;

import nl.uva.vlet.exception.VlException;
import nl.uva.vlet.vfs.VFSFactory;
import nl.uva.vlet.vfs.VFileSystem;
import nl.uva.vlet.vrl.VRL;
import nl.uva.vlet.vrs.ServerInfo;
import nl.uva.vlet.vrs.VRSContext;

public class CloudFSFactory extends VFSFactory {

    private CloudFileSystem cloudFS;

    @Override
    public VFileSystem createNewFileSystem(VRSContext context, ServerInfo info,
            VRL location) throws VlException {
        cloudFS = new CloudFileSystem(context, info);
        return cloudFS;
    }

    @Override
    public String getName() {
        return "Cloud";
    }

    @Override
    public String[] getSchemeNames() {
        return Constants.SCHEMES;
    }

    @Override
    public void clear() {
        if (cloudFS != null) {
            cloudFS.dispose();
        }
    }

    @Override
    protected ServerInfo updateServerInfo(VRSContext context, ServerInfo info,
            VRL loc) throws VlException {
        // check update ServerInfo here
        // String userInf=loc.getUserinfo();
        // info.setUsername(userInf);
        // info.store()
        // Set Authentication information

        info.setUsePasswordAuth();
        info.store();

        return info;
    }
}
