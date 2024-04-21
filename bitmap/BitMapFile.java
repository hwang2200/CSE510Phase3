package bitmap;

import btree.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import bufmgr.*;
import global.*;
import columnar.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import value.*;

public class BitMapFile
		implements GlobalConst {
	private static FileOutputStream fos;
	private static DataOutputStream trace;

	private BitMapHeaderPage headerPage;
	private PageId headerPageId;
	private String bmfilename;
	private final static int MAGIC0=1989;



	public BitMapFile(String filename)
			throws GetFileEntryException,
			ConstructPageException, IOException {



		headerPageId = get_file_entry(filename);
		headerPage = new BitMapHeaderPage(headerPageId);
		bmfilename = filename;


	}

	public BitMapFile(String filename, Columnarfile columnarFile, int ColumnNo, ValueClass value)
			throws GetFileEntryException,
			ConstructPageException,
			IOException,
			AddFileEntryException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException {

		//File not found is expected so we can create new header page
		headerPageId = get_file_entry(filename);
		if (headerPageId == null)
		{
			headerPage = new BitMapHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_colNum(ColumnNo);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			bmfilename = filename;

			// Sets the header key for the value type
			if (value instanceof IntegerValueClass)
			{
				headerPage.set_keyType((short) 0);
			}
			else if (value instanceof StringValueClass)
			{
				headerPage.set_keyType((short) 1);
			}
		}
		else
		{
			headerPage = new BitMapHeaderPage(headerPageId);
			bmfilename = filename;

			// Sets the header key for the value type
			if (value instanceof IntegerValueClass)
			{
				headerPage.set_keyType((short) 0);
			}
			else if (value instanceof StringValueClass)
			{
				headerPage.set_keyType((short) 1);
			}
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
			PageId pgId = headerPage.getPageId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(bmfilename);
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

	public boolean Delete(int position, int bitmapRange) {
		try {
			int key = headerPage.get_keyType();

			BMPage page = new BMPage();
			PageId nextpageno = headerPage.getNextPage();

			while (nextpageno.pid != INVALID_PAGE) {
				page.setCurPage(nextpageno);
				nextpageno = page.getNextPage();
			}

			byte[] data;

			if (key == 0 || key == 1) {
				data = new byte[bitmapRange];
				data[position] = 0;
			} else {
				data = new byte[4];
				// TODO: Modify headerpage so it stores information about how to insert using

			}

			if ((key == 0 && page.available_space() >= 2) || (key == 1 && page.available_space() >= 4)) {
				// Page exists with space
				page.writeBMPageArray(data);

			} else {
				// TODO Need to add a new page
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

	public boolean Insert(int position, int bitmapRange)
			throws IOException {
		try {
			int key = headerPage.get_keyType();

			BMPage page = new BMPage();
			PageId apage = new PageId();

			//if no header page, need to create new one
			if(headerPage.get_rootId().pid == INVALID_PAGE)
			{
				//Create new page and page structure
				page.setNextPage(new PageId(INVALID_PAGE));
				headerPage.set_rootId(page.getCurPage());


				page = new BMPage(headerPage);
				this.headerPageId = page.getCurPage();

				page.setNextPage(new PageId(INVALID_PAGE));
				page.setPrevPage(new PageId(INVALID_PAGE));

				PageId nextpageno = headerPage.getNextPage();

				while (nextpageno.pid != INVALID_PAGE)
				{
					page.setCurPage(nextpageno);
					nextpageno = page.getNextPage();
				}

				//Prepare data to add
				byte[] data;

				if (key == 0 || key == 1) {
					data = new byte[bitmapRange];
					//TODO
					//System.out.println("Page doesn't exist");
					//System.out.println("Byte index: " + byteIndex);
					//System.out.println("Bit index: " + bitIndex);

					data[position] = 1;
				}
				else
				{
					data = new byte[4];
					// TODO: Modify headerpage so it stores information about how to insert using

				}

				//TODO
				//System.out.println(Arrays.toString(data));

				if ((key == 0 && page.available_space() >= 2) || (key == 1 && page.available_space() >= 4)) {
					// Page exists with space
					page.writeBMPageArray(data);
				}
				else
				{
					//TODO Need to add a new page
					//Right after the key becomes 14, reaches here

					BMPage newpage = new BMPage();
					page.setNextPage(newpage.getCurPage());
					newpage.setPrevPage(page.getCurPage());
					newpage.writeBMPageArray(data);
				}

			}
			//header page exists (simply insert the record)
			else
			{
				byte[] data;
				if (key == 0 || key == 1) {
					data = new byte[bitmapRange];

					//TODO
					//System.out.println("Page does exist");
					//System.out.println("Byte index: " + byteIndex);
					//System.out.println("Bit index: " + bitIndex);

					data[position] = 1;
				}
				else
				{
					data = new byte[4];
					// TODO: Modify headerpage so it stores information about how to insert using

				}
				//TODO print bitmap array

				page.writeBMPageArray(data);

			}


			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean CInsert(byte[] data)
	{
		//Length of bitmap array will always be 6
		//Should insert each array into the page
		try {
			int key = headerPage.get_keyType();

			BMPage page = new BMPage();
			PageId apage = new PageId();

			//if no header page, need to create new one
			if(headerPage.get_rootId().pid == INVALID_PAGE) {
				//Create new page and page structure
				page.setNextPage(new PageId(INVALID_PAGE));
				headerPage.set_rootId(page.getCurPage());


				page = new BMPage(headerPage);
				this.headerPageId = page.getCurPage();

				page.setNextPage(new PageId(INVALID_PAGE));
				page.setPrevPage(new PageId(INVALID_PAGE));

				PageId nextpageno = headerPage.getNextPage();

				while (nextpageno.pid != INVALID_PAGE) {
					page.setCurPage(nextpageno);
					nextpageno = page.getNextPage();
				}
				if ((key == 0 && page.available_space() >= 2) || (key == 1 && page.available_space() >= 4)) {
					// Page exists with space
					page.writeBMPageArray(data);
				}
			}
			else
			{
				page.writeBMPageArray(data);
			}
			return true;
		} catch(Exception e){
			return false;
		}
	}

	public BitMapHeaderPage getHeaderPage() {
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

			PageId result = SystemDefs.JavabaseDB.get_file_entry(filename);
			return result;
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
