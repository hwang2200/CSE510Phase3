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

    void set_rootId(PageId rootID)
            throws IOException {
        setNextPage(rootID);
    }

    PageId get_rootId()
            throws IOException {
        return getNextPage();
    }

    void set_keyType(short key_type)
            throws IOException {
        setSlot(3, (int) key_type, 0);
    }

    short get_keyType()
            throws IOException {
        return (short) getSlotLength(3);
    }

    void set_maxKeySize(int key_size)
            throws IOException {
        setSlot(1, key_size, 0);
    }

    int get_maxKeySize()
            throws IOException {
        return getSlotLength(1);
    }

    void set_deleteFashion(int fashion)
            throws IOException {
        setSlot(2, fashion, 0);
    }

    int get_deleteFashion()
            throws IOException {
        return getSlotLength(2);
    }

}
