/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

@SuppressWarnings({ "OctalInteger" })
public class FakeFilesystem implements Filesystem3, XattrSupport,
		LifecycleSupport {
	private static final Logger log = Logger.getLogger(FakeFilesystem.class
			.getName());

	private static final int BLOCK_SIZE = 512;
	private static final int NAME_LENGTH = 1024;

	private static class Node {
		static int nfiles = 0;

		String name;
		int mode;
		Map<String, byte[]> xattrs = new HashMap<String, byte[]>();

		Node(final String name, final int mode, final String... xattrs) {
			this.name = name;
			this.mode = mode;

			for (int i = 0; i < xattrs.length - 1; i += 2) {
				this.xattrs.put(xattrs[i], xattrs[i + 1].getBytes());
			}

			nfiles++;
		}

		@Override
		public String toString() {
			final String cn = getClass().getName();
			return cn.substring(cn.indexOf("$")) + "[ name=" + name + ", mode="
					+ Integer.toOctalString(mode) + "(OCT) ]";
		}
	}

	private static class Directory extends Node {
		Map<String, Node> files = new LinkedHashMap<String, Node>();

		Directory(final String name, final int mode, final String... xattrs) {
			super(name, mode, xattrs);
		}

		void add(final Node node) {
			files.put(node.name, node);
		}

		@Override
		public String toString() {
			return super.toString() + " with " + files.size() + " files";
		}
	}

	private static class File extends Node {
		byte[] content;

		File(final String name, final int mode, final String content,
				final String... xattrs) {
			super(name, mode, xattrs);

			this.content = content.getBytes();
		}
	}

	private static class Link extends Node {
		String link;

		Link(final String name, final int mode, final String link,
				final String... xattrs) {
			super(name, mode, xattrs);

			this.link = link;
		}
	}

	private static class FileHandle {
		Node node;

		FileHandle(final Node node) {
			this.node = node;
			log.info("  " + this + " created");
		}

		void release() {
			log.info("  " + this + " released");
		}

		@Override
		protected void finalize() {
			log.info("  " + this + " finalized");
		}

		@Override
		public String toString() {
			return "FileHandle[" + node + ", hashCode=" + hashCode() + "]";
		}
	}

	// a root directory
	private final Directory root;

	// lookup node

	private Node lookup(final String path) {
		if (path.equals("/")) {
			return root;
		}

		final java.io.File f = new java.io.File(path);
		final Node parent = lookup(f.getParent());
		final Node node = (parent instanceof Directory) ? ((Directory) parent).files
				.get(f.getName()) : null;

		log.info("  lookup(\"" + path + "\") returning: " + node);

		return node;
	}

	public FakeFilesystem() {
		root = new Directory("", 0755, "description", "ROOT directory");

		root.add(new File("README", 0644, "You have read me\n", "mimetype",
				"text/plain", "description", "a README file"));
		root.add(new File("execute_me.sh", 0755,
				"#!/bin/sh\n\necho \"You executed me\"\n", "mimetype",
				"text/plain", "description", "a BASH script"));

		final Directory subdir = new Directory("subdir", 0755, "description",
				"a subdirectory");
		root.add(subdir);
		subdir.add(new Link("README.link", 0666, "../README", "description",
				"a symbolic link"));
		subdir.add(new Link("execute_me.link.sh", 0666, "../execute_me.sh",
				"description", "another symbolic link"));

		log.info("created");
	}

	public int chmod(final String path, final int mode) throws FuseException {
		final Node node = lookup(path);

		if (node != null) {
			node.mode = (node.mode & FuseStatConstants.TYPE_MASK)
					| (mode & FuseStatConstants.MODE_MASK);
			return 0;
		}

		return Errno.ENOENT;
	}

	public int chown(final String path, final int uid, final int gid)
			throws FuseException {
		return 0;
	}

	public int getattr(final String path, final FuseGetattrSetter getattrSetter)
			throws FuseException {
		final Node node = lookup(path);

		final int time = (int) (System.currentTimeMillis() / 1000L);

		if (node instanceof Directory) {
			final Directory directory = (Directory) node;
			getattrSetter.set(directory.hashCode(), FuseFtypeConstants.TYPE_DIR
					| directory.mode, 1, 0, 0, 0, directory.files.size()
					* NAME_LENGTH, (directory.files.size() * NAME_LENGTH
					+ BLOCK_SIZE - 1)
					/ BLOCK_SIZE, time, time, time, time);

			return 0;
		} else if (node instanceof File) {
			final File file = (File) node;
			getattrSetter.set(file.hashCode(), FuseFtypeConstants.TYPE_FILE
					| file.mode, 1, 0, 0, 0, file.content.length,
					(file.content.length + BLOCK_SIZE - 1) / BLOCK_SIZE, time,
					time, time, time);

			return 0;
		} else if (node instanceof Link) {
			final Link link = (Link) node;
			getattrSetter.set(link.hashCode(), FuseFtypeConstants.TYPE_SYMLINK
					| link.mode, 1, 0, 0, 0, link.link.length(),
					(link.link.length() + BLOCK_SIZE - 1) / BLOCK_SIZE, time,
					time, time, time);

			return 0;
		}

		return Errno.ENOENT;
	}

	public int getdir(final String path, final FuseDirFiller filler)
			throws FuseException {
		final Node node = lookup(path);

		if (node instanceof Directory) {
			for (final Node child : ((Directory) node).files.values()) {
				final int ftype = (child instanceof Directory) ? FuseFtypeConstants.TYPE_DIR
						: ((child instanceof File) ? FuseFtypeConstants.TYPE_FILE
								: ((child instanceof Link) ? FuseFtypeConstants.TYPE_SYMLINK
										: 0));
				if (ftype > 0) {
					filler.add(child.name, child.hashCode(), ftype | child.mode);
				}
			}

			return 0;
		}

		return Errno.ENOTDIR;
	}

	public int link(final String from, final String to) throws FuseException {
		return Errno.EROFS;
	}

	public int mkdir(final String path, final int mode) throws FuseException {
		return Errno.EROFS;
	}

	public int mknod(final String path, final int mode, final int rdev)
			throws FuseException {
		return Errno.EROFS;
	}

	public int rename(final String from, final String to) throws FuseException {
		return Errno.EROFS;
	}

	public int rmdir(final String path) throws FuseException {
		return Errno.EROFS;
	}

	public int statfs(final FuseStatfsSetter statfsSetter) throws FuseException {
		statfsSetter.set(BLOCK_SIZE, 1000, 200, 180, Node.nfiles, 0,
				NAME_LENGTH);
		return 0;
	}

	public int symlink(final String from, final String to) throws FuseException {
		return Errno.EROFS;
	}

	public int truncate(final String path, final long size)
			throws FuseException {
		return Errno.EROFS;
	}

	public int unlink(final String path) throws FuseException {
		return Errno.EROFS;
	}

	public int utime(final String path, final int atime, final int mtime)
			throws FuseException {
		return 0;
	}

	public int readlink(final String path, final CharBuffer link)
			throws FuseException {
		final Node node = lookup(path);

		if (node instanceof Link) {
			link.append(((Link) node).link);
			return 0;
		}

		return Errno.ENOENT;
	}

	// if open returns a filehandle by calling FuseOpenSetter.setFh() method, it
	// will be passed to every method that supports 'fh' argument

	public int open(final String path, final int flags,
			final FuseOpenSetter openSetter) throws FuseException {
		final Node node = lookup(path);

		if (node != null) {
			openSetter.setFh(new FileHandle(node));
			return 0;
		}

		return Errno.ENOENT;
	}

	// fh is filehandle passed from open,
	// isWritepage indicates that write was caused by a writepage

	public int write(final String path, final Object fh,
			final boolean isWritepage, final ByteBuffer buf, final long offset)
			throws FuseException {
		return Errno.EROFS;
	}

	// fh is filehandle passed from open

	public int read(final String path, final Object fh, final ByteBuffer buf,
			final long offset) throws FuseException {
		if (fh instanceof FileHandle) {
			final File file = (File) ((FileHandle) fh).node;
			buf.put(file.content,
					(int) offset,
					Math.min(buf.remaining(), file.content.length
							- (int) offset));

			return 0;
		}

		return Errno.EBADF;
	}

	// new operation (called on every filehandle close), fh is filehandle passed
	// from open

	public int flush(final String path, final Object fh) throws FuseException {
		if (fh instanceof FileHandle) {
			return 0;
		}

		return Errno.EBADF;
	}

	// new operation (Synchronize file contents), fh is filehandle passed from
	// open,
	// isDatasync indicates that only the user data should be flushed, not the
	// meta data

	public int fsync(final String path, final Object fh,
			final boolean isDatasync) throws FuseException {
		if (fh instanceof FileHandle) {
			return 0;
		}

		return Errno.EBADF;
	}

	// (called when last filehandle is closed), fh is filehandle passed from
	// open

	public int release(final String path, final Object fh, final int flags)
			throws FuseException {
		if (fh instanceof FileHandle) {
			((FileHandle) fh).release();
			System.runFinalization();
			return 0;
		}

		return Errno.EBADF;
	}

	//
	// XattrSupport implementation

	/**
	 * This method will be called to get the value of the extended attribute
	 * 
	 * @param path
	 *            the path to file or directory containing extended attribute
	 * @param name
	 *            the name of the extended attribute
	 * @param dst
	 *            a ByteBuffer that should be filled with the value of the
	 *            extended attribute
	 * @param position
	 *            specifies the offset within the extended attribute.
	 * @return 0 if Ok or errno when error
	 * @throws fuse.FuseException
	 *             an alternative to returning errno is to throw this exception
	 *             with errno initialized
	 * @throws java.nio.BufferOverflowException
	 *             should be thrown to indicate that the given <code>dst</code>
	 *             ByteBuffer is not large enough to hold the attribute's value.
	 *             After that <code>getxattr()</code> method will be called
	 *             again with a larger buffer.
	 */
	public int getxattr(final String path, final String name,
			final ByteBuffer dst, final int position) throws FuseException,
			BufferOverflowException {
		final Node node = lookup(path);

		if (node == null) {
			return Errno.ENOENT;
		}

		final byte[] value = node.xattrs.get(name);

		if (value == null) {
			return Errno.ENOATTR;
		}

		dst.put(value);

		return 0;
	}

	/**
	 * This method can be called to query for the size of the extended attribute
	 * 
	 * @param path
	 *            the path to file or directory containing extended attribute
	 * @param name
	 *            the name of the extended attribute
	 * @param sizeSetter
	 *            a callback interface that should be used to set the
	 *            attribute's size
	 * @return 0 if Ok or errno when error
	 * @throws fuse.FuseException
	 *             an alternative to returning errno is to throw this exception
	 *             with errno initialized
	 */
	public int getxattrsize(final String path, final String name,
			final FuseSizeSetter sizeSetter) throws FuseException {
		final Node node = lookup(path);

		if (node == null) {
			return Errno.ENOENT;
		}

		final byte[] value = node.xattrs.get(name);

		if (value == null) {
			return Errno.ENOATTR;
		}

		sizeSetter.setSize(value.length);

		return 0;
	}

	/**
	 * This method will be called to get the list of extended attribute names
	 * 
	 * @param path
	 *            the path to file or directory containing extended attributes
	 * @param lister
	 *            a callback interface that should be used to list the attribute
	 *            names
	 * @return 0 if Ok or errno when error
	 * @throws fuse.FuseException
	 *             an alternative to returning errno is to throw this exception
	 *             with errno initialized
	 */
	public int listxattr(final String path, final XattrLister lister)
			throws FuseException {
		final Node node = lookup(path);

		if (node == null) {
			return Errno.ENOENT;
		}

		for (final String xattrName : node.xattrs.keySet()) {
			lister.add(xattrName);
		}

		return 0;
	}

	/**
	 * This method will be called to remove the extended attribute
	 * 
	 * @param path
	 *            the path to file or directory containing extended attributes
	 * @param name
	 *            the name of the extended attribute
	 * @return 0 if Ok or errno when error
	 * @throws fuse.FuseException
	 *             an alternative to returning errno is to throw this exception
	 *             with errno initialized
	 */
	public int removexattr(final String path, final String name)
			throws FuseException {
		return Errno.EROFS;
	}

	/**
	 * This method will be called to set the value of an extended attribute
	 * 
	 * @param path
	 *            the path to file or directory containing extended attributes
	 * @param name
	 *            the name of the extended attribute
	 * @param value
	 *            the value of the extended attribute
	 * @param flags
	 *            parameter can be used to refine the semantics of the
	 *            operation.
	 *            <p>
	 *            <code>XATTR_CREATE</code> specifies a pure create, which
	 *            should fail with <code>Errno.EEXIST</code> if the named
	 *            attribute exists already.
	 *            <p>
	 *            <code>XATTR_REPLACE</code> specifies a pure replace operation,
	 *            which should fail with <code>Errno.ENOATTR</code> if the named
	 *            attribute does not already exist.
	 *            <p>
	 *            By default (no flags), the extended attribute will be created
	 *            if need be, or will simply replace the value if the attribute
	 *            exists.
	 * @param position
	 *            specifies the offset within the extended attribute.
	 * @return 0 if Ok or errno when error
	 * @throws fuse.FuseException
	 *             an alternative to returning errno is to throw this exception
	 *             with errno initialized
	 */
	public int setxattr(final String path, final String name,
			final ByteBuffer value, final int flags, final int position)
			throws FuseException {
		return Errno.EROFS;
	}

	//
	// LifeCycleSupport

	public int init() {
		log.info("Initializing Filesystem");
		return 0;
	}

	public int destroy() {
		log.info("Destroying Filesystem");
		return 0;
	}

	//
	// Java entry point

	public static void main(final String[] args) {
		log.info("entering");

		try {
			FuseMount.mount(args, new FakeFilesystem(), log);
		} catch (final Exception e) {
			e.printStackTrace();
		} finally {
			log.info("exiting");
		}
	}
}
