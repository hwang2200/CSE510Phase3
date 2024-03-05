package columnar;

import diskmgr.Page;
import global.PageId;
import global.SystemDefs;
import heap.*;
import TID.*;

import java.io.IOException;

public class TupleScan
{
    private Columnarfile _cf;
    private boolean nextUserStatus;
    private HFPage datapage = new HFPage();
    private HFPage dirpage = new HFPage();

    //Constructor
    public TupleScan(Columnarfile cf) throws InvalidTupleSizeException
    {
        init(cf);
    }

    //Methods
    public void closetuplescan()
    {

    }

    public Tuple getNext(TID tid) throws InvalidTupleSizeException, IOException
    {
        Tuple recptrtuple = null;

        if (nextUserStatus != true)
        {
            nextDataPage();
        }

        if (datapage == null)
        {
            return null;
        }

        //Change to numRIDs, position, and array of record IDs
        tid.pageNo.pid = userrid.pageNo.pid;
        rid.slotNo = userrid.slotNo;

        try
        {
            recptrtuple = datapage.getRecord(rid);
        }
        catch (Exception e)
        {
            //    System.err.println("SCAN: Error in Scan" + e);
            e.printStackTrace();
        }

        userrid = datapage.nextRecord(rid);
        if(userrid == null) nextUserStatus = false;
        else nextUserStatus = true;

        return recptrtuple;
    }

    public boolean position(TID tid)
    {

    }

    private void reset()
    {
        if (datapage != null)
        {
            try
            {
                unpinPage(datapageId, false);
            }
            catch (Exception e)
            {
                // 	System.err.println("SCAN: Error in Scan" + e);
                e.printStackTrace();
            }
        }
        datapageId.pid = 0;
        datapage = null;

        if (dirpage != null)
        {
            try
            {
                unpinPage(dirpageId, false);
            }
            catch (Exception e)
            {
                //     System.err.println("SCAN: Error in Scan: " + e);
                e.printStackTrace();
            }
        }
        dirpage = null;
        nextUserStatus = true;
    }

    private boolean nextDataPage() throws InvalidTupleSizeException, IOException
    {

    }

    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Scan.java: pinPage() failed");
        }

    } // end of pinPage

    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Scan.java: unpinPage() failed");
        }

    } // end of unpinPage
}
