package bitmap;

import btree.*;
import java.io.*;

import columnar.Columnarfile;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import value.ValueClass;

public class BitMapFile implements GlobalConst {
	private BitMapHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	public BitMapFile(java.lang.String filename) 
		throws GetFileEntryException
	{
		headerPageId = get_file_entry(filename);   
		headerPage = new BitMapHeaderPage(headerPageId);       
		dbname = new String(filename);
	}
	
	public BitMapFile(java.lang.String filename, Columnarfile columnfile, int ColumnNo, ValueClass value)
		throws GetFileEntryException,
		ConstructPageException,
		IOException,
		AddFileEntryException
	{
		headerPageId = get_file_entry(filename);
		if (headerPageId == null) {
			headerPage = new BitMapHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
		
			

			// headerPage.set_magic0(MAGIC0);
			// headerPage.set_rootId(new PageId(INVALID_PAGE));
			// headerPage.set_keyType((short)keytype);    
			// headerPage.set_maxKeySize(keysize);
			// headerPage.set_deleteFashion(delete_fashion);
			// headerPage.setType(NodeType.BTHEAD);
		} else {
			headerPage = new BitMapHeaderPage(headerPageId);
		}

		dbname = new String(filename);

	}
	
	public void close() 
		throws PageUnpinnedException,
		InvalidFrameNumberException,
		HashEntryNotFoundException,
		ReplacerException
	{
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
		PinPageException   
	{
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
		FreePageException
	{

	}

	public boolean Delete(int position) {
		// Set the entry at the given position to 0
		
		return true;
	}
	
	public boolean Insert(int position) {
		// Set the entry at the given position to 1
		
		return true;
	}
	
	private bitmap.BitMapHeaderPage getHeaderPage() {
		return headerPage;
	}

	private void add_file_entry(String fileName, PageId pageno) 
    	throws AddFileEntryException
    {
      	try {
        	SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
      	} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e,"");
      	}      
    }

	private PageId get_file_entry(String filename)
		throws GetFileEntryException 
	{
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}
	
	private void delete_file_entry(String filename)
		throws DeleteFileEntryException
	{
		try {
			SystemDefs.JavabaseDB.delete_file_entry( filename );
	 	} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e,"");
		} 
	}

	private void unpinPage(PageId pageno)
		throws UnpinPageException
    { 
    	try {
        	SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);    
      	} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e,"");
      	} 
    }

	private void freePage(PageId pageno) 
    	throws FreePageException
    {
      	try {
			SystemDefs.JavabaseBM.freePage(pageno);    
      	} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e,"");
      	} 
    }
}
