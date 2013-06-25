/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse;

import java.util.logging.Logger;

import fuse.compat.Filesystem1;
import fuse.compat.Filesystem1ToFilesystem2Adapter;
import fuse.compat.Filesystem2;
import fuse.compat.Filesystem2ToFilesystem3Adapter;

public class FuseMount {

	final static Logger log = Logger.getLogger(FuseMount.class.getName());

	static {
		System.loadLibrary("javafs");
	}

	private FuseMount() {
		// no instances
	}

	//
	// compatibility APIs
	public static void mount(final String[] args, final Filesystem1 filesystem1)
			throws Exception {
		mount(args, new Filesystem2ToFilesystem3Adapter(
				new Filesystem1ToFilesystem2Adapter(filesystem1)),
				Logger.getLogger(filesystem1.getClass().getName()));
	}

	public static void mount(final String[] args, final Filesystem2 filesystem2)
			throws Exception {
		mount(args, new Filesystem2ToFilesystem3Adapter(filesystem2),
				Logger.getLogger(filesystem2.getClass().getName()));
	}

	//
	// prefered String level API
	public static void mount(final String[] args,
			final Filesystem3 filesystem3, final Logger log) throws Exception {
		mount(args, new Filesystem3ToFuseFSAdapter(filesystem3, log));
	}

	//
	// byte level API
	public static void mount(final String[] args, final FuseFS fuseFS)
			throws Exception {
		final ThreadGroup threadGroup = new ThreadGroup(Thread.currentThread()
				.getThreadGroup(), "FUSE Threads");
		threadGroup.setDaemon(true);

		log.info("Mounting filesystem");

		mount(args, fuseFS, threadGroup);

		log.info("Filesystem is unmounted");

		if (log != null) {
			final int n = threadGroup.activeCount();
			log.fine("ThreadGroup(\"" + threadGroup.getName()
					+ "\").activeCount() = " + n);

			final Thread[] threads = new Thread[n];
			threadGroup.enumerate(threads);
			for (int i = 0; i < threads.length; i++) {
				log.fine("thread[" + i + "] = " + threads[i] + ", isDaemon = "
						+ threads[i].isDaemon());
			}
		}
	}

	//
	// byte level API
	public static void mount(final String[] args,
			final Filesystem3 filesystem3, final ThreadGroup group,
			final Logger log) throws Exception {

		final Filesystem3ToFuseFSAdapter fuseFS = new Filesystem3ToFuseFSAdapter(
				filesystem3, log);
		final Thread fuseThread = new Thread(group, new Runnable() {
			public void run() {
				try {
					log.info("Mounting filesystem");
					mount(args, fuseFS, group);
					log.info("Filesystem is unmounted");
					if ((log != null)) {
						final int n = group.activeCount();
						log.fine("ThreadGroup(\"" + group.getName()
								+ "\").activeCount() = " + n);

						final Thread[] threads = new Thread[n];
						group.enumerate(threads);
						for (int i = 0; i < threads.length; i++) {
							log.fine("thread[" + i + "] = " + threads[i]
									+ ", isDaemon = " + threads[i].isDaemon());
						}
					}
				} catch (final Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
		group.setDaemon(true);
		fuseThread.setDaemon(true);
		fuseThread.start();
	}

	private static native void mount(String[] args, FuseFS fuseFS,
			ThreadGroup threadGroup) throws Exception;
}
