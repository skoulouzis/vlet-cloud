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
 * $Id: VTestCase.java,v 1.1 2012-02-09 16:50:51 skoulouz Exp $  
 * $Date: 2012-02-09 16:50:51 $
 */
// source: 

package nl.uva.vlet.vfs.test;

import junit.framework.TestCase;
import nl.uva.vlet.vfs.VFSClient;

/**
 * My own subclasses VTestCase. Added some conveniance methods.
 * 
 * @author P.T. de Boer
 */
public class VTestCase extends TestCase {
	public static final int VERBOSE_NONE = 0;

	public static final int VERBOSE_ = 1;

	public static final int VERBOSE_WARN = 2;

	public static final int VERBOSE_INFO = 3;

	public static final int VERBOSE_DEBUG = 4;

	static int verboseLevel = VERBOSE_INFO;

	public static void setVerbose(int level) {
		verboseLevel = level;
	}

	public static void verbose(int verbose, String msg) {
		if (verbose <= verboseLevel)
			System.out.println("testVFS:" + msg);
	}

	public static void message(String msg) {
		verbose(VERBOSE_INFO, msg);
	}

	public static void warning(String msg) {
		verbose(VERBOSE_WARN, msg);
	}

	public static void debug(String msg) {
		verbose(VERBOSE_DEBUG, msg);
	}

	public static synchronized void staticCheckProxy() {
		// if ((VRSContext.getDefault().getGridProxy().isValid() == false))
		// GridProxyDialog.askInitProxy("Grid Proxy needed for Junit tests");
	}

	// ===
	// Instance
	// ===

	private VFSClient vfs = new VFSClient();

	public VFSClient getVFS() {
		return vfs;
	}

}
