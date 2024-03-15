package bitmap;

import btree.*;
import java.io.*;
import bufmgr.*;
import global.*;
import columnar.*;
import value.*;
import diskmgr.*;
import heap.*;

public class BitMapFile
		implements GlobalConst {
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
			AddFileEntryException,
			InvalidTupleSizeException {
		
		headerPageId = get_file_entry(filename);
		if (headerPageId == null) {
			headerPage = new BitMapHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
		} else
			headerPage = new BitMapHeaderPage(headerPageId);

		dbname = new String(filename);

		Page page = new Page();

		// Sets the header key for the value type
		if (value instanceof IntegerValueClass) {
			headerPage.set_keyType((short) 0);
		} else if (value instanceof StringValueClass) {
			headerPage.set_keyType((short) 1);
		}

		// Initialize the first page
		page.setpage(null);
		BMPage bmPage = new BMPage(page);
		bmPage.setNextPage(new PageId(INVALID_PAGE));
		bmPage.setPrevPage(headerPageId);

		headerPage.setNextPage(bmPage.getCurPage());

		Scan scan = columnfile.openColumnScan(ColumnNo);
		Tuple data = scan.getNext(scan.getUserRID());

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
			PageId pgId = headerPage.getPageId();
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
		BMPage page = new BMPage();

		page.setCurPage(pageno);

		PageId nextpageno = pageno;
		PageId deletepageno;

		while (nextpageno.pid != INVALID_PAGE) {
			deletepageno = page.getCurPage();

			page.setCurPage(nextpageno);
			nextpageno = page.getNextPage();

			freePage(deletepageno);
			unpinPage(deletepageno);
		}
	}

	public boolean Delete(int position) {
		try {
			int key = (int) headerPage.get_keyType();

			BMPage page = new BMPage();
			PageId nextpageno = headerPage.getNextPage();

			while (nextpageno.pid != INVALID_PAGE) {
				page.setCurPage(nextpageno);
				nextpageno = page.getNextPage();
			}

			byte[] data;

			if (key == 0) {
				data = new byte[2];
				int byteIndex = position / 8;
				int bitIndex = position % 8;
				data[byteIndex] |= 0 << bitIndex;
			} else {
				data = new byte[4];
				// TODO: Modify headerpage so it stores information about how to insert using
				
			}

			if ((key == 0 && page.available_space() >= 2) || (key == 1 && page.available_space() >= 4)) {
				// Page exists with space
				page.writeBMPageArray(data);

			} else {
				// Need to add a new page
				BMPage newpage = new BMPage();
				page.setNextPage(newpage.getCurPage());
				newpage.setPrevPage(page.getCurPage());
				newpage.writeBMPageArray(data);
			}

			return true;		
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean Insert(int position)
			throws IOException {
		try {
			int key = (int) headerPage.get_keyType();

			BMPage page = new BMPage();
			PageId nextpageno = headerPage.getNextPage();

			while (nextpageno.pid != INVALID_PAGE) {
				page.setCurPage(nextpageno);
				nextpageno = page.getNextPage();
			}

			byte[] data;

			if (key == 0) {
				data = new byte[2];
				int byteIndex = position / 8;
				int bitIndex = position % 8;
				data[byteIndex] |= 1 << bitIndex;
			} else {
				data = new byte[4];
				// TODO: Modify headerpage so it stores information about how to insert using
				
			}

			if ((key == 0 && page.available_space() >= 2) || (key == 1 && page.available_space() >= 4)) {
				// Page exists with space
				page.writeBMPageArray(data);

			} else {
				// Need to add a new page
				BMPage newpage = new BMPage();
				page.setNextPage(newpage.getCurPage());
				newpage.setPrevPage(page.getCurPage());
				newpage.writeBMPageArray(data);
			}

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public bitmap.BitMapHeaderPage getHeaderPage() {
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
