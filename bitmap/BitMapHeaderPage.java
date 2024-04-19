package bitmap;

import java.io.*;

import btree.*;
import diskmgr.*;
import global.*;
import heap.*;

public class BitMapHeaderPage extends HFPage {
    public BitMapHeaderPage() throws ConstructPageException {
        super();
        try {
            Page apage = new Page();
            PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
            if (pageId == null)
                throw new ConstructPageException(null, "new page failed");
            this.init(pageId, apage);
        } catch (Exception e) {
            throw new ConstructPageException(e, "construct header page failed");
        }
    }

    public BitMapHeaderPage(PageId pageno) throws ConstructPageException {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, this, false);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pinpage failed");
        }
    }

    void set_magic0(int magic)
            throws IOException {
        setPrevPage(new PageId(magic));
    }

    int get_magic0()
            throws IOException {
        return getPrevPage().pid;
    };

    void setPageId(PageId pageno)
            throws IOException {
        setCurPage(pageno);
    }

    PageId getPageId()
            throws IOException {
        return getCurPage();
    }

    void set_keyType(short key_type)
            throws IOException {
        setSlot(30, (int) key_type, 0);
    }

    short get_keyType()
            throws IOException {
        return (short) getSlotLength(30);
    }

    void set_colNum(int colNum) throws IOException {
        setSlot(1, colNum, 0);
    }

    int get_colNum(int colNum) throws IOException {
        return getSlotLength(1);
    }

    void  set_rootId( PageId rootID )
            throws IOException
    {
        setNextPage(rootID);
    }

    public PageId get_rootId()
            throws IOException
    {
        return getNextPage();
    }



}
