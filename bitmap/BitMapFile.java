package bitmap;

import btree.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import bufmgr.*;
import diskmgr.PCounter;
import diskmgr.Page;
import global.*;
import columnar.*;
import heap.*;
import value.*;

public class BitMapFile
		implements GlobalConst {
	private static FileOutputStream fos;
	private static DataOutputStream trace;

	private BitMapHeaderPage headerPage;
	private PageId headerPageId;
	public String bmfilename;

	private final static int MAGIC0=1989;



	public BitMapFile(String filename)
			throws GetFileEntryException,
			ConstructPageException, IOException {

		headerPageId = get_file_entry(filename);
		headerPage = new BitMapHeaderPage(headerPageId);
		bmfilename = filename;

		if(headerPage.firstPID == -1)
		{
			headerPage.firstPID = headerPageId.pid;
		}
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
		}
		else
		{
			headerPage = new BitMapHeaderPage(headerPageId);
		}
		if(headerPage.firstPID == -1)
		{
			headerPage.firstPID = headerPageId.pid;
		}

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

	public boolean Delete(int position, int bitmapRange, boolean compressed) throws IOException, FreePageException, ConstructPageException, UnpinPageException {

		int key = headerPage.get_keyType();
		String bmfilename;
		if(!compressed) {
			bmfilename = this.bmfilename;
		}
		else
		{
			bmfilename = this.bmfilename;
		}
		BitMapFile tmpBMF = null;
		int count = 0;
		try {
			tmpBMF = new BitMapFile(bmfilename);
		} catch (GetFileEntryException | ConstructPageException | IOException e) {
			throw new RuntimeException(e);
		}

		PageId firstPage = new PageId(tmpBMF.getHeaderPage().firstPID + 1);
		int currPID = firstPage.pid;
		int lastPID = tmpBMF.getHeaderPage().get_rootId().pid;
		Page pg1 = null;
		try
		{
			pg1 = tmpBMF.pinPage(firstPage);
		} catch (btree.PinPageException e) {
			throw new RuntimeException(e);
		}



		int tmpPos = 0;
		do {
			BMPage bmpage = new BMPage(pg1);

			RID tmpRID = new RID();
			try {
				tmpRID = bmpage.firstRecord();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			Tuple t = new Tuple();
			List<byte[]> slotArray = new ArrayList<>();

			boolean changed = false;
			do {
				boolean matchFound = false;
				try {
					t = bmpage.getRecord(tmpRID);
				} catch (IOException | InvalidSlotNumberException e) {
					throw new RuntimeException(e);
				}
				byte[] data = t.getTupleByteArray();


				//handle string case
				if (key == AttrType.attrString) {
					if(!compressed) {
						if (data[position] == 1) {
							count++;
							System.out.println("Data[" + tmpPos + "]: ");
							changed = true;
							matchFound = true;
						}
					}
					else {
						int posValue = position - 1;
						if (data[0] == posValue) {
							count++;
							System.out.println("Data[" + tmpPos + "]: ");
							changed = true;
							matchFound = true;
						}
					}
				} else { //handling int case
					if(!compressed) {
						if (data[position] == 1) {
							count++;
							System.out.println("Data[" + tmpPos + "]");
							changed = true;
							matchFound = true;
						}
					}
					else {
						if (data[0] == position) {
							count++;
							System.out.println("Data[" + tmpPos + "]");
							changed = true;
							matchFound = true;
						}
					}
				}

				try {
					tmpRID = bmpage.nextRecord(tmpRID);
					if(tmpRID != null) { tmpPos++; }
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				if(!matchFound) {
					slotArray.add(data);
				}
			} while (tmpRID  != null);

			if(changed) //need to create a new page and overwrite
			{

				PageId prevPage = bmpage.getPrevPage();
				PageId nextPage = bmpage.getNextPage();
				BMPage newBMPage = new BMPage();
				try{
					Page apage=new Page();
					PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
					if (pageId==null)
						throw new ConstructPageException(null, "construct new page failed");
					newBMPage.init(pageId, apage);
				}
				catch (Exception e) {
					e.printStackTrace();
					throw new ConstructPageException(e, "construct sorted page failed");
				}
				newBMPage.setPrevPage(prevPage);

				Page ppage = null;
				try {
					ppage = pinPage(prevPage);
				} catch (Exception e) {
					e.printStackTrace();
				}
				assert ppage != null;
				BMPage page = new BMPage(ppage);
				page.setNextPage(newBMPage.getCurPage());
				unpinPage(prevPage);

				newBMPage.setNextPage(nextPage);
				freePage(bmpage.getCurPage());
				unpinPage(bmpage.getCurPage());
				bmpage = newBMPage;
				for(int i = 0; i < slotArray.size(); i++)
				{
					bmpage.writeBMPageArray(slotArray.get(i));
				}
			}

			try {
				tmpBMF.unpinPage(new PageId(currPID));
			}catch (Exception e)
			{
				throw new RuntimeException(e);
			}

			try {
				currPID++;
				if(currPID > lastPID) { break; }
				bmpage.setCurPage(new PageId(currPID ));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
//                try {
//                    if(bmpage.getCurPage().pid == INVALID_PAGE) { break; }
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }


			try {
				pg1 = tmpBMF.pinPage(bmpage.getCurPage());
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}while(true);

		System.out.println(count + " matches found for value constraint " + position);

		return true;
	}

	public boolean Insert(int position, int bitmapRange, boolean compressed)
			throws IOException {

		try {
			int key = headerPage.get_keyType();

			byte[] data = null;

			if(!compressed) {
				if (key == 0 || key == 1) { //0 = int; 1 = str
					data = new byte[bitmapRange];
					data[position] = 1;
				} else {
					System.err.println("Key value invalid on bitmap insert: " + key);
					return false;
				}
			}
			else
			{
				List<int[]> uncompressedArray = new ArrayList<>();

				if(key == 0 || key == 1)
				{
					int[] uncompressed = new int[bitmapRange];
					uncompressed[position] = 1;
					uncompressedArray.add(uncompressed);

					for(int[] array : uncompressedArray)
					{
						int leadingZeros = 0;
						int trailingZeros = 0;

						for(int i = 0; i < array.length; i++) {
							if(array[i] == 0) {
								leadingZeros += 1;
							} else if (array[i] == 1) {
								break;
							}
						}

						for(int i = array.length - 1; i >= 0; i--) {
							if(array[i] == 0) {
								trailingZeros += 1;
							} else if (array[i] == 1) {
								break;
							}
						}

						//Each even index will have count, each odd index will have value
						int[] CBitmap = {leadingZeros, 0, 1, 1, trailingZeros, 0};
						data = new byte[6];

						//TODO
//						System.out.println("For " + Arrays.toString(array) + ": " + leadingZeros + ", " + trailingZeros);
//						System.out.println("CBitmap for ints: " + Arrays.toString(CBitmap));
						for (int i = 0; i < CBitmap.length; i++)
						{
							data[i] = (byte)CBitmap[i];
						}
					}
				}
				else
				{
					System.err.println("Key value invalid on bitmap insert: " + key);
					return false;
				}
			}

			if(data == null)
			{
				System.err.println("Data not set: NULL");
				return false;
			}
			//if no header page, need to create new one
			if(headerPage.get_rootId().pid == INVALID_PAGE)
			{
				//Create new page and page structure
				PageId newPageID = new PageId();
				BMPage page = new BMPage();
				try{
					Page apage=new Page();
					PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
					if (pageId==null)
						throw new ConstructPageException(null, "construct new page failed");
					page.init(pageId, apage);
				}
				catch (Exception e) {
					e.printStackTrace();
					throw new ConstructPageException(e, "construct sorted page failed");
				}
				page.setNextPage(new PageId(INVALID_PAGE));
				page.setPrevPage(new PageId(INVALID_PAGE));
				newPageID = page.getCurPage();
				headerPage.set_rootId(page.getCurPage());

				//Prepare data to add


				if ((key == 0 && page.available_space() >= (bitmapRange + 4)) || (key == 1 && page.available_space() >= (bitmapRange + 4))) {
					// Page exists with space
					page.writeBMPageArray(data);
				}
				else
				{
					//TODO Need to add a new page
					//Right after the key becomes 14, reaches here

					BMPage newpage = new BMPage();
					page.setNextPage(new PageId(INVALID_PAGE));
					headerPage.set_rootId(page.getCurPage());
					newpage.setPrevPage(newPageID);
					newpage.writeBMPageArray(data);
				}
				unpinPage(newPageID, true);
				updateHeader(newPageID);

			}
			//header page exists (simply insert the record)
			else
			{

				PageId p = headerPage.get_rootId();
				//apage = headerPage.set_rootId(pid);
				Page pg1 = null;
				try {
					pg1 = pinPage(p);
				} catch (Exception e) {
					e.printStackTrace();
				}
                assert pg1 != null;
                BMPage page = new BMPage(pg1);
				page.setNextPage(new PageId(INVALID_PAGE));



				//System.out.println("PageID: " + page.getCurPage().pid + " space: " + page.available_space());
				if ((key == 0 && page.available_space() >= (bitmapRange + 4)) || (key == 1 && page.available_space() >= (bitmapRange + 4))) {
					// Page exists with space
					page.writeBMPageArray(data);

				}
				else {
					//not enough space, need to create new page
					PageId newPageId = new PageId();
					BMPage newBMPage = new BMPage();
					try{
						Page apage=new Page();
						PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
						if (pageId==null)
							throw new ConstructPageException(null, "construct new page failed");
						newBMPage.init(pageId, apage);
					}
					catch (Exception e) {
						e.printStackTrace();
						throw new ConstructPageException(e, "construct sorted page failed");
					}
					newBMPage.setPrevPage(page.getCurPage());
					newPageId = newBMPage.getCurPage();
					page.setNextPage(newBMPage.getCurPage());
					newBMPage.setNextPage(new PageId(INVALID_PAGE));

//					System.out.println("New Page created");
//					System.out.println("Prev Page: " + newBMPage.getPrevPage());
//					System.out.println("Curr Page: " + newBMPage.getCurPage());
//					System.out.println("Next Page: " + newBMPage.getNextPage());
//					System.out.println();

					if ((key == 0 && newBMPage.available_space() >= (bitmapRange + 4)) || (key == 1 && newBMPage.available_space() >= (bitmapRange + 4))) {
						// Page exists with space
						newBMPage.writeBMPageArray(data);
					}

					unpinPage(newPageId, true);
					updateHeader(newPageId);
				}
				try {
					unpinPage(p);
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}

			return true;
		} catch (Exception e) {
			e.printStackTrace();
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

	public Page pinPage(PageId pageno)
			throws btree.PinPageException
	{
		try {
			Page page=new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
			return page;
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new btree.PinPageException(e,"");
		}
	}

	public void unpinPage(PageId pageno)
			throws UnpinPageException
	{
		try{
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e,"");
		}
	}

	public void unpinPage(PageId pageno, boolean dirty)
			throws UnpinPageException
	{
		try{
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e,"");
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

	private void  updateHeader(PageId newRoot)
			throws IOException,
			btree.PinPageException,
			UnpinPageException, ConstructPageException {

		BitMapHeaderPage header;
		PageId old_data;


		header= new BitMapHeaderPage( pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId( newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */ );


		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}


	@Override
	public String toString()
	{
		int count = 0;
		BitMapFile tmpBMF = null;
		try {
			System.out.println(this.bmfilename);
			tmpBMF = new BitMapFile(this.bmfilename);
		} catch (GetFileEntryException | ConstructPageException | IOException e) {
			throw new RuntimeException(e);
		}
		PageId firstPage = new PageId(tmpBMF.getHeaderPage().firstPID + 1);
		Page pg1 = null;
		try
		{
			pg1 = pinPage(firstPage);
		} catch (btree.PinPageException e) {
            throw new RuntimeException(e);
        }

		do {
			BMPage bmpage = new BMPage(pg1);
			try {
				System.out.println("BMPage: " + bmpage.getCurPage().pid);
				System.out.println("Next BMPage: " + bmpage.getNextPage().pid);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			RID tmpRID = new RID();
			try {
				tmpRID = bmpage.firstRecord();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			Tuple t = new Tuple();

			do {
				try {
					t = bmpage.getRecord(tmpRID);
				} catch (IOException | InvalidSlotNumberException e) {
					throw new RuntimeException(e);
				}
                byte[] data = t.getTupleByteArray();
				System.out.println("Data[" + count + "]: " + Arrays.toString(data));
				count++;
				try {
					tmpRID = bmpage.nextRecord(tmpRID);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} while (tmpRID  != null);

			try {
				unpinPage(bmpage.getCurPage());
			}catch (Exception e)
			{
				throw new RuntimeException(e);
			}

			try {
				bmpage.setCurPage(bmpage.getNextPage());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			try {
				if(bmpage.getCurPage().pid == INVALID_PAGE) { break; }
			} catch (IOException e) {
				throw new RuntimeException(e);
			}



			try {
				pg1 = pinPage(bmpage.getCurPage());
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}while(true);


		return "";
	}

}
