/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva.vlet.vfs.test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import nl.uva.vlet.Global;
import nl.uva.vlet.GlobalConfig;
import nl.uva.vlet.exception.VRLSyntaxException;
import nl.uva.vlet.exception.VlException;
import nl.uva.vlet.vfs.VDir;
import nl.uva.vlet.vfs.VFSClient;
import nl.uva.vlet.vfs.VFSNode;
import nl.uva.vlet.vrl.VRL;
import nl.uva.vlet.vrs.ServerInfo;
import nl.uva.vlet.vrs.VNode;
import nl.uva.vlet.vrs.VRS;
import nl.uva.vlet.vrs.VRSContext;

/**
 * 
 * @author S. Koulouzis
 */
public class TestDriver {

	public static void main(String args[]) {
		try {
//			test1();
                        
                        
                        test2();
                        
		} catch (Exception ex) {
			Logger.getLogger(TestDriver.class.getName()).log(Level.SEVERE,
					null, ex);
		} finally {
			VRS.exit();
		}
	}

	private static void test1() throws MalformedURLException, Exception {

		GlobalConfig.setBaseLocation(new URL("http://dummy/url"));
		// runtime configuration
		GlobalConfig.setHasUI(false);
		GlobalConfig.setIsApplet(true);
		GlobalConfig.setPassiveMode(true);
		GlobalConfig.setIsService(true);
		GlobalConfig.setInitURLStreamFactory(false);
		GlobalConfig.setAllowUserInteraction(false);

		// user configuration
		// GlobalConfig.setUsePersistantUserConfiguration(false);
		// GlobalConfig.setUserHomeLocation(new URL("file:////" +
		// this.tmpVPHuserHome.getAbsolutePath()));
		// Global.setDebug(true);

		VRS.getRegistry().addVRSDriverClass(
				nl.uva.vlet.vfs.cloud.CloudFSFactory.class);

		String[] supportedSchemes = VRS.getRegistry().getDefaultSchemeNames();

		for (String s : supportedSchemes) {
			System.out.println("Supported storage: " + s);
		}

		Global.init();

		VFSClient vfsClient = new VFSClient();
		VRSContext context = vfsClient.getVRSContext();

		VRL vrl = new VRL("swift://149.156.10.131:8443/auth/v1.0/");
		ServerInfo info = context.getServerInfoFor(vrl, true);

		info.setUsername("vph_dev:admin");

		info.setPassword("passwd");

		info.setAttribute(ServerInfo.ATTR_DEFAULT_YES_NO_ANSWER, true);
		ServerInfo newOne = info.store();

		context.updateServerInfo(newOne);
		vfsClient.setVRSContext(context);

		// ServerInfo newInfo = context.getServerInfoFor(vrl, true);

		System.out.println("Uname and passwd: " + newOne.getUsername() + " "
				+ newOne.getPassword());

		VNode loc = vfsClient.openLocation(vrl);
		System.out.println("Opened: " + loc.getVRL());

		if (loc instanceof VDir) {
			VDir dir = (VDir) loc;
			VFSNode[] children = dir.list();
			for (VFSNode n : children) {
				System.out.println("List: " + n.getVRL());
			}
		}

		// VRSFactory rf = context.getResourceFactoryFor(new
		// VRL("swift://149.156.10.131:8443/auth/v1.0/testBlobStoreVFS"));
		// System.out.println("ResourceFactory: " + rf.getClass().getName());
		// for (String s : rf.getResourceTypes()) {
		// System.out.println("Types: " + s);
		// }

		// VNode loc = rf.openLocation(context,
		// "swift://149.156.10.131:8443/auth/v1.0/testBlobStoreVFS");

		// System.out.println("Opened: " + loc.getVRL());

	}

    private static void test2() throws VRLSyntaxException, VlException, Exception {
        	GlobalConfig.setBaseLocation(new URL("http://dummy/url"));
		// runtime configuration
		GlobalConfig.setHasUI(false);
		GlobalConfig.setIsApplet(true);
		GlobalConfig.setPassiveMode(true);
		GlobalConfig.setIsService(true);
		GlobalConfig.setInitURLStreamFactory(false);
		GlobalConfig.setAllowUserInteraction(false);

		// user configuration
		// GlobalConfig.setUsePersistantUserConfiguration(false);
		// GlobalConfig.setUserHomeLocation(new URL("file:////" +
		// this.tmpVPHuserHome.getAbsolutePath()));
		// Global.setDebug(true);

		VRS.getRegistry().addVRSDriverClass(
				nl.uva.vlet.vfs.cloud.CloudFSFactory.class);

		String[] supportedSchemes = VRS.getRegistry().getDefaultSchemeNames();

		for (String s : supportedSchemes) {
			System.out.println("Supported storage: " + s);
		}

		Global.init();

		VFSClient vfsClient = new VFSClient();
		VRSContext context = vfsClient.getVRSContext();

		VRL vrl = new VRL("swift://149.156.10.131:8443/auth/v1.0/");
		ServerInfo info = context.getServerInfoFor(vrl, true);

		info.setUsername("domain:role");

		info.setPassword("secret");

		info.setAttribute(ServerInfo.ATTR_DEFAULT_YES_NO_ANSWER, true);
		info.store();

		 vfsClient.existsPath(vrl.append("someDir"));
    }
}
