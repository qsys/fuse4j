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
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is an adapter that implements fuse.FuseFS byte level API and delegates
 * to the fuse.Filesystem3 String level API. You specify the encoding to be used
 * for file names and paths.
 */
public class Filesystem3ToFuseFSAdapter implements FuseFS {
	private final Filesystem3 fs3;
	private XattrSupport xattrSupport;
	private LifecycleSupport lifecycleSupport;

	private final Charset cs;
	private final Logger log;

	public Filesystem3ToFuseFSAdapter(final Filesystem3 fs3, final Logger log) {
		this(fs3, System.getProperty("file.encoding", "UTF-8"), log);
	}

	public Filesystem3ToFuseFSAdapter(final Filesystem3 fs3,
			final String encoding, final Logger log) {
		this(fs3, Charset.forName(encoding), log);
	}

	public Filesystem3ToFuseFSAdapter(final Filesystem3 fs3, final Charset cs,
			final Logger log) {
		this.fs3 = fs3;

		// XattrSupport is optional
		if (fs3 instanceof XattrSupport) {
			xattrSupport = (XattrSupport) fs3;
		}

		// Lifecycle is optional
		if (fs3 instanceof LifecycleSupport) {
			lifecycleSupport = (LifecycleSupport) fs3;
		}

		this.cs = cs;
		this.log = log;
	}

	//
	// FuseFS implementation

	public int getattr(final ByteBuffer path,
			final FuseGetattrSetter getattrSetter) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("getattr: path=" + pathStr);
		}

		try {
			return handleErrno(fs3.getattr(pathStr, getattrSetter),
					getattrSetter);
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int readlink(final ByteBuffer path, final ByteBuffer link) {
		final String pathStr = cs.decode(path).toString();
		if (log != null) {
			log.fine("readlink: path=" + pathStr);
		}

		final CharBuffer linkCb = CharBuffer.allocate(link.capacity());

		try {
			final int errno = fs3.readlink(pathStr, linkCb);

			if (errno == 0) {
				linkCb.flip();

				final CharsetEncoder enc = cs.newEncoder()
						.onUnmappableCharacter(CodingErrorAction.REPLACE)
						.onMalformedInput(CodingErrorAction.REPLACE);

				final CoderResult result = enc.encode(linkCb, link, true);
				if (result.isOverflow()) {
					throw new FuseException(
							"Buffer owerflow while encoding result")
							.initErrno(Errno.ENAMETOOLONG);
				}
			}

			return handleErrno(errno, linkCb.rewind());
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int getdir(final ByteBuffer path, final FuseFSDirFiller dirFiller) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("getdir: path=" + pathStr);
		}

		try {
			dirFiller.setCharset(cs);
			return handleErrno(fs3.getdir(pathStr, dirFiller), dirFiller);
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int mknod(final ByteBuffer path, final int mode, final int rdev) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("mknod: path=" + pathStr + ", mode="
					+ Integer.toOctalString(mode) + "(OCT), rdev=" + rdev);
		}

		try {
			return handleErrno(fs3.mknod(pathStr, mode, rdev));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int mkdir(final ByteBuffer path, final int mode) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("mkdir: path=" + pathStr + ", mode="
					+ Integer.toOctalString(mode) + "(OCT)");
		}

		try {
			return handleErrno(fs3.mkdir(pathStr, mode));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int unlink(final ByteBuffer path) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("unlink: path=" + pathStr);
		}

		try {
			return handleErrno(fs3.unlink(pathStr));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int rmdir(final ByteBuffer path) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("rmdir: path=" + pathStr);
		}

		try {
			return handleErrno(fs3.rmdir(pathStr));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int symlink(final ByteBuffer from, final ByteBuffer to) {
		final String fromStr = cs.decode(from).toString();
		final String toStr = cs.decode(to).toString();

		if (log != null) {
			log.fine("symlink: from=" + fromStr + " to=" + toStr);
		}

		try {
			return handleErrno(fs3.symlink(fromStr, toStr));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int rename(final ByteBuffer from, final ByteBuffer to) {
		final String fromStr = cs.decode(from).toString();
		final String toStr = cs.decode(to).toString();

		if (log != null) {
			log.fine("rename: from=" + fromStr + " to=" + toStr);
		}

		try {
			return handleErrno(fs3.rename(fromStr, toStr));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int link(final ByteBuffer from, final ByteBuffer to) {
		final String fromStr = cs.decode(from).toString();
		final String toStr = cs.decode(to).toString();

		if (log != null) {
			log.fine("link: from=" + fromStr + " to=" + toStr);
		}

		try {
			return handleErrno(fs3.link(fromStr, toStr));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int chmod(final ByteBuffer path, final int mode) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("chmod: path=" + pathStr + ", mode="
					+ Integer.toOctalString(mode) + "(OCT)");
		}

		try {
			return handleErrno(fs3.chmod(pathStr, mode));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int chown(final ByteBuffer path, final int uid, final int gid) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("chown: path=" + pathStr + ", uid=" + uid + ", gid=" + gid);
		}

		try {
			return handleErrno(fs3.chown(pathStr, uid, gid));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int truncate(final ByteBuffer path, final long size) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("truncate: path=" + pathStr + ", size=" + size);
		}

		try {
			return handleErrno(fs3.truncate(pathStr, size));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int utime(final ByteBuffer path, final int atime, final int mtime) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("utime: path=" + pathStr + ", atime=" + atime + " ("
					+ new Date(atime * 1000L) + "), mtime=" + mtime + " ("
					+ new Date(mtime * 1000L) + ")");
		}

		try {
			return handleErrno(fs3.utime(pathStr, atime, mtime));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int statfs(final FuseStatfsSetter statfsSetter) {
		if (log != null) {
			log.fine("statfs");
		}

		try {
			return handleErrno(fs3.statfs(statfsSetter), statfsSetter);
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int open(final ByteBuffer path, final int flags,
			final FuseOpenSetter openSetter) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("open: path=" + pathStr + ", flags=" + flags);
		}

		try {
			return handleErrno(fs3.open(pathStr, flags, openSetter), openSetter);
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int read(final ByteBuffer path, final Object fh,
			final ByteBuffer buf, final long offset) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("read: path=" + pathStr + ", fh=" + fh + ", offset="
					+ offset);
		}

		try {
			return handleErrno(fs3.read(pathStr, fh, buf, offset), buf);
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int write(final ByteBuffer path, final Object fh,
			final boolean isWritepage, final ByteBuffer buf, final long offset) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("write: path=" + pathStr + ", fh=" + fh + ", isWritepage="
					+ isWritepage + ", offset=" + offset);
		}

		try {
			return handleErrno(
					fs3.write(pathStr, fh, isWritepage, buf, offset), buf);
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int flush(final ByteBuffer path, final Object fh) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("flush: path=" + pathStr + ", fh=" + fh);
		}

		try {
			return handleErrno(fs3.flush(pathStr, fh));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int release(final ByteBuffer path, final Object fh, final int flags) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("release: path=" + pathStr + ", fh=" + fh + ", flags="
					+ flags);
		}

		try {
			return handleErrno(fs3.release(pathStr, fh, flags));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int fsync(final ByteBuffer path, final Object fh,
			final boolean isDatasync) {
		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("fsync: path=" + pathStr + ", fh=" + fh + ", isDatasync="
					+ isDatasync);
		}

		try {
			return handleErrno(fs3.fsync(cs.decode(path).toString(), fh,
					isDatasync));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	//
	// extended attribute support is optional

	public int getxattrsize(final ByteBuffer path, final ByteBuffer name,
			final FuseSizeSetter sizeSetter) {
		if (xattrSupport == null) {
			return handleErrno(Errno.ENOTSUPP);
		}

		final String pathStr = cs.decode(path).toString();
		final String nameStr = cs.decode(name).toString();

		if (log != null) {
			log.fine("getxattrsize: path=" + pathStr + ", name=" + nameStr);
		}

		try {
			return handleErrno(
					xattrSupport.getxattrsize(pathStr, nameStr, sizeSetter),
					sizeSetter);
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int getxattr(final ByteBuffer path, final ByteBuffer name,
			final ByteBuffer value, final int position) {
		if (xattrSupport == null) {
			return handleErrno(Errno.ENOTSUPP);
		}

		final String pathStr = cs.decode(path).toString();
		final String nameStr = cs.decode(name).toString();

		if (log != null) {
			log.fine("getxattr: path=" + pathStr + ", name=" + nameStr);
		}

		try {
			return handleErrno(
					xattrSupport.getxattr(pathStr, nameStr, value, position),
					value);
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	//
	// private implementation of XattrLister that estimates the byte size of the
	// attribute names list
	// using Charset of the enclosing Filesystem3ToFuseFSAdapter class

	private class XattrSizeLister implements XattrLister {
		CharsetEncoder enc = cs.newEncoder();
		int size = 0;

		public void add(final String xattrName) {
			try {
				size += xattrName.getBytes("UTF-8").length + 1;
			} catch (final Exception e) {
				handleException(e);
			}

		}
	}

	//
	// estimate the byte size of attribute names list...

	public int listxattrsize(final ByteBuffer path,
			final FuseSizeSetter sizeSetter) {
		if (xattrSupport == null) {
			return handleErrno(Errno.ENOTSUPP);
		}

		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("listxattrsize: path=" + pathStr);
		}

		int errno;
		final XattrSizeLister lister = new XattrSizeLister();

		try {
			errno = xattrSupport.listxattr(pathStr, lister);
		} catch (final Exception e) {
			return handleException(e);
		}

		sizeSetter.setSize(lister.size);

		return handleErrno(errno, sizeSetter);
	}

	//
	// private implementation of XattrLister that encodes list of attribute
	// names into given ByteBuffer
	// using Charset of the enclosing Filesystem3ToFuseFSAdapter class

	private class XattrValueLister implements XattrLister {
		CharsetEncoder enc = cs.newEncoder()
				.onMalformedInput(CodingErrorAction.REPLACE)
				.onUnmappableCharacter(CodingErrorAction.REPLACE);
		ByteBuffer list;
		BufferOverflowException boe;

		XattrValueLister(final ByteBuffer list) {
			this.list = list;
		}

		public void add(final String xattrName) {
			if (boe == null) // don't need to bother any more if there was an
								// exception already
			{
				try {
					enc.encode(CharBuffer.wrap(xattrName + "\u0000"), list,
							true);
				} catch (final BufferOverflowException e) {
					boe = e;
				}
			}
		}

		//
		// for debugging

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();

			sb.append("[");
			boolean first = true;

			for (int i = 0; i < list.position(); i++) {
				final int offset = i;
				int length = 0;
				while (offset + length < list.position()
						&& list.get(offset + length) != 0) {
					length++;
				}

				final byte[] nameBytes = new byte[length];
				for (int j = 0; j < length; j++) {
					nameBytes[j] = list.get(offset + j);
				}

				if (first) {
					first = false;
				} else {
					sb.append(", ");
				}

				sb.append('"').append(cs.decode(ByteBuffer.wrap(nameBytes)))
						.append('"');

				i = offset + length;
			}

			sb.append("]");

			return sb.toString();
		}
	}

	//
	// list attributes into given ByteBuffer...

	public int listxattr(final ByteBuffer path, final ByteBuffer list) {
		if (xattrSupport == null) {
			return handleErrno(Errno.ENOTSUPP);
		}

		final String pathStr = cs.decode(path).toString();

		if (log != null) {
			log.fine("listxattr: path=" + pathStr);
		}

		int errno;
		final XattrValueLister lister = new XattrValueLister(list);

		try {
			errno = xattrSupport.listxattr(pathStr, lister);
		} catch (final Exception e) {
			return handleException(e);
		}

		// was there a BufferOverflowException?
		if (lister.boe != null) {
			return handleException(lister.boe);
		}

		return handleErrno(errno, lister);
	}

	public int setxattr(final ByteBuffer path, final ByteBuffer name,
			final ByteBuffer value, final int flags, final int position) {
		if (xattrSupport == null) {
			return handleErrno(Errno.ENOTSUPP);
		}

		final String pathStr = cs.decode(path).toString();
		final String nameStr = cs.decode(name).toString();

		if (log != null) {
			log.fine("setxattr: path=" + pathStr + ", name=" + nameStr
					+ ", value=" + value + ", flags=" + flags);
		}

		try {
			return handleErrno(xattrSupport.setxattr(pathStr, nameStr, value,
					flags, position));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int removexattr(final ByteBuffer path, final ByteBuffer name) {
		if (xattrSupport == null) {
			return handleErrno(Errno.ENOTSUPP);
		}

		final String pathStr = cs.decode(path).toString();
		final String nameStr = cs.decode(name).toString();

		if (log != null) {
			log.fine("removexattr: path= " + pathStr + ", name=" + nameStr);
		}

		try {
			return handleErrno(xattrSupport.removexattr(pathStr, nameStr));
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	// Lifecycle support is optional
	public int init() {
		if (lifecycleSupport == null) {
			return handleErrno(Errno.ENOTSUPP);
		}

		if (log != null) {
			log.fine("init: start filesystem");
		}

		try {
			return handleErrno(lifecycleSupport.init());
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	public int destroy() {
		if (lifecycleSupport == null) {
			return handleErrno(Errno.ENOTSUPP);
		}

		if (log != null) {
			log.fine("destroy: shutdown filesystem");
		}

		try {
			return handleErrno(lifecycleSupport.destroy());
		} catch (final Exception e) {
			return handleException(e);
		}
	}

	//
	// private
	private int handleErrno(final int errno) {
		if (log != null) {
			log.fine((errno == 0) ? "  returning with success"
					: "  returning errno: " + errno);
		}

		return errno;
	}

	private int handleErrno(final int errno, final Object v1) {
		if (errno != 0) {
			return handleErrno(errno);
		}

		if (log != null) {
			log.fine("  returning: " + v1);
		}

		return errno;

	}

	private int handleErrno(final int errno, final Object v1, final Object v2) {
		if (errno != 0) {
			return handleErrno(errno);
		}

		if (log != null) {
			log.fine("  returning: " + v1 + ", " + v2);
		}

		return errno;

	}

	private int handleException(final Exception e) {
		int errno;

		if (e instanceof FuseException) {
			errno = handleErrno(((FuseException) e).getErrno());
			if (log != null) {
				log.log(Level.FINE, "Fuse Exception", e);
			}
		} else if (e instanceof BufferOverflowException) {
			errno = handleErrno(Errno.ERANGE);
			if (log != null) {
				log.log(Level.FINE, "Buffer Overflow", e);
			}
		} else {
			errno = handleErrno(Errno.EFAULT);
			if (log != null) {
				log.log(Level.WARNING, "EFAULT", e);
			}
		}

		return errno;
	}
}
