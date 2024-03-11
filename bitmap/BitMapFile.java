package bitmap;

import btree.*;
import java.io.*;
import bufmgr.*;
import global.*;
import columnar.*;
import value.*;

public class BitMapFile extends IndexFile
		implements GlobalConst {
	private final static int MAGIC0 = 2024;
	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	private BitMapHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	public BitMapFile(java.lang.String filename)
			throws GetFileEntryException, 
			PinPageException, 
			ConstructPageException {
		headerPageId = get_file_entry(filename);
		headerPage = new BitMapHeaderPage(headerPageId);
		dbname = new String(filename);
	}

	public BitMapFile(java.lang.String filename, Columnarfile columnfile, int ColumnNo, ValueClass value)
			throws GetFileEntryException,
			ConstructPageException,
			IOException,
			AddFileEntryException {
		// ints 0-9 or 20 distinct strings of max 25 chars
		headerPageId = get_file_entry(filename);
		if (headerPageId == null) {
			headerPage = new BitMapHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
		} else {
			headerPage = new BitMapHeaderPage(headerPageId);
		}

		dbname = new String(filename);

		if (columnfile.type[ColumnNo] == new AttrType(AttrType.attrInteger)) {
			headerPage.set_keyType();
		} else if (columnfile.type[ColumnNo] == new AttrType(AttrType.attrString)) {

		} 

	}

	public static void traceFilename(String filename)
			throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}

	public static void destroyTrace()
			throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

	public void close()
			throws PageUnpinnedException,
			InvalidFrameNumberException,
			HashEntryNotFoundException,
			ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	public void destroyBitMapFile()
			throws IOException,
			IteratorException,
			UnpinPageException,
			FreePageException,
			DeleteFileEntryException,
			ConstructPageException,
			PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno)
			throws IOException,
			IteratorException,
			PinPageException,
			ConstructPageException,
			UnpinPageException,
			FreePageException {

	}

	public boolean Delete(int position) {
		// Set the entry at the given position to 0

		return true;
	}

	public boolean Delete(KeyClass key, RID rid) {

		return true;
	}

	public boolean Insert(int position) {
		// Set the entry at the given position to 1

		return true;
	}

	public void insert(KeyClass key, RID rid) {

	}

	private bitmap.BitMapHeaderPage getHeaderPage() {
		return headerPage;
	}

	private void add_file_entry(String fileName, PageId pageno)
			throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private PageId get_file_entry(String filename)
			throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private void delete_file_entry(String filename)
			throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno)
			throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno)
			throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}
	}
}
