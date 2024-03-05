package bitmap;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

public class BitMapHeaderPage {
    public BitMapHeaderPage() {

    }
    
    public BitMapHeaderPage(PageId pageno) {
        
    }

    PageId getPageId()
        throws IOException
    {
        return getCurPage();
    }

    void set_rootId(PageId rootID)
        throws IOException
    {
        setNextPage(rootID);
    }

    PageId get_rootId()
        throws IOException
    {
        return getNextPage();
    }

    void set_keyType(short key_type)
        throws IOException
    {
        setSlot(3, (int)key_type, 0);
    }
    
    void set_maxKeySize(int key_size)
        throws IOException
    {
        setSlot(1, key_size, 0)
    }
}
