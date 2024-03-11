package bitmap;

import java.io.*;
import java.lang.*;

import global.*;
import diskmgr.*;

interface ConstSlot{
    int INVALID_SLOT =  -1;
    int EMPTY_SLOT = -1;
  }

public class BMPage extends Page
    implements ConstSlot, GlobalConst{
  
  
        public static final int SIZE_OF_SLOT = 4;
        public static final int DPFIXED =  4 * 2  + 3 * 4;
        
        public static final int SLOT_CNT = 0;
        public static final int USED_PTR = 2;
        public static final int FREE_SPACE = 4;
        public static final int TYPE = 6;
        public static final int PREV_PAGE = 8;
        public static final int NEXT_PAGE = 12;
        public static final int CUR_PAGE = 16;
        
        /* Warning:
           These items must all pack tight, (no padding) for
           the current implementation to work properly.
           Be careful when modifying this class.
        */
        
        /**
         * number of slots in use
         */
        private    short     slotCnt;  
        
        /**
         * offset of first used byte by data records in data[]
         */
        private    short     usedPtr;   
        
        /**
         * number of bytes free in data[]
         */
        private    short     freeSpace;  
        
        /**
         * an arbitrary value used by subclasses as needed
         */
        private    short     type;     
        
        /**
         * backward pointer to data page
         */
        private    PageId   prevPage = new PageId(); 
        
        /**
         * forward pointer to data page
         */
        private   PageId    nextPage = new PageId();  
        
        /**
         *  page number of this page
         */
        protected    PageId    curPage = new PageId();   
    // default constructor
    public BMPage () {

    }

    // constructor 
    // open a page and make this BMPage point to a given page
    public BMPage (Page page) {
        data = page.getpage();
    }

    // constructor
    // open an existing BMPage from the buffer pool
    public void openBMpage (Page aPage){
        data = aPage.getpage();
    }

    // constructor
    // initialize a new page with the given parameters

    public void init (PageId pageNo, Page apage)
    throws IOException
    {
        data = apage.getpage();
      
        slotCnt = 0;                // no slots in use
        Convert.setShortValue (slotCnt, SLOT_CNT, data);
        
        curPage.pid = pageNo.pid;
        Convert.setIntValue (curPage.pid, CUR_PAGE, data);
        
        nextPage.pid = prevPage.pid = INVALID_PAGE;
        Convert.setIntValue (prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue (nextPage.pid, NEXT_PAGE, data);
        
        usedPtr = (short) MAX_SPACE;  // offset in data array (grow backwards)
        Convert.setShortValue (usedPtr, USED_PTR, data);
        
        freeSpace = (short) (MAX_SPACE - DPFIXED);    // amount of space available
        Convert.setShortValue (freeSpace, FREE_SPACE, data);
        
    }

    public byte [] getBMpageArray (){
        return data;
    }

    // dump contents of the given page

    public void dumpPage()     
    throws IOException
    {
        int i, n;
        int length, offset;
        
        curPage.pid =  Convert.getIntValue (CUR_PAGE, data);
        nextPage.pid =  Convert.getIntValue (NEXT_PAGE, data);
        usedPtr =  Convert.getShortValue (USED_PTR, data);
        freeSpace =  Convert.getShortValue (FREE_SPACE, data);
        slotCnt =  Convert.getShortValue (SLOT_CNT, data);
        
        System.out.println("dumpPage");
        System.out.println("curPage= " + curPage.pid);
        System.out.println("nextPage= " + nextPage.pid);
        System.out.println("usedPtr= " + usedPtr);
        System.out.println("freeSpace= " + freeSpace);
        System.out.println("slotCnt= " + slotCnt);
        
        for (i= 0, n=DPFIXED; i < slotCnt; n +=SIZE_OF_SLOT, i++) {
            length =  Convert.getShortValue (n, data);
            offset =  Convert.getShortValue (n+2, data);
            System.out.println("slotNo " + i +" offset= " + offset);
            System.out.println("slotNo " + i +" length= " + length);
        }
        
    }
    /**
    * get previous page
    * @return pageId of previous page
    */
    public PageId getPrevPage()   
    throws IOException {
        prevPage.pid =  Convert.getIntValue (PREV_PAGE, data);
        return prevPage;
    }

    /**
    * sets value of prevPage to pageNo
    * @param       pageNo  page number for previous page
    * @exception IOException I/O errors
    */
    public void setPrevPage(PageId pageNo)
    throws IOException
    {
        prevPage.pid = pageNo.pid;
        Convert.setIntValue (prevPage.pid, PREV_PAGE, data);
    }

    /**
    * @return     page number of next page
    * @exception IOException I/O errors
    */
    public PageId getNextPage()
    throws IOException
    {
        nextPage.pid =  Convert.getIntValue (NEXT_PAGE, data);    
        return nextPage;
    }

    /**
    * sets value of nextPage to pageNo
    * @param	pageNo	page number for next page
    * @exception IOException I/O errors
    */
    public void setNextPage(PageId pageNo)   
    throws IOException
    {
        nextPage.pid = pageNo.pid;
        Convert.setIntValue (nextPage.pid, NEXT_PAGE, data);
    }

    /**
    * @return 	page number of current page
    * @exception IOException I/O errors
    */
    public PageId getCurPage() 
    throws IOException
    {
        curPage.pid =  Convert.getIntValue (CUR_PAGE, data);
        return curPage;
    }

    /**
     * sets value of curPage to pageNo
     * @param	pageNo	page number for current page
     * @exception IOException I/O errors
     */
    public void setCurPage(PageId pageNo)   
    throws IOException
    {
        curPage.pid = pageNo.pid;
        Convert.setIntValue (curPage.pid, CUR_PAGE, data);
    }

    /**
     * returns the amount of available space on the page.
     * @return  the amount of available space on the page
     * @exception  IOException I/O errors
     */  
    public int available_space()  
    throws IOException
    {
        freeSpace = Convert.getShortValue (FREE_SPACE, data);
        return (freeSpace - SIZE_OF_SLOT);
    }

    /**      
     * Determining if the page is empty
     * @return true if the BMPage is has no records in it, false otherwise  
     * @exception  IOException I/O errors
     */
    public boolean empty() 
    throws IOException
    {
        int i;
        short length;
        // look for an empty slot
        slotCnt = Convert.getShortValue (SLOT_CNT, data);
        
        for (i= 0; i < slotCnt; i++) 
    {
        length = getSlotLength(i);
        if (length != EMPTY_SLOT)
        return false;
    }    
        
        return true;
    }

    /**
     * @param	slotno	slot number
     * @exception IOException I/O errors
     * @return	the length of record the given slot contains
     */
    public short getSlotLength(int slotno)
    throws IOException
    {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        short val= Convert.getShortValue(position, data);
        return val;
    }

    /**
     * sets slot contents
     * @param slotno slot number
     * @param length length of the slot
     * @param offset offset of the input
     * @throws IOException
     */
    public void setSlot(int slotno, int length, int offset)
    throws IOException
    {
      int position = DPFIXED + slotno * SIZE_OF_SLOT;
      Convert.setShortValue((short)length, position, data);
      Convert.setShortValue((short)offset, position+2, data);
    }
    /**
     * writes the byte array given
     * @param byteWrite
     */
    public void writeBMPageArray(byte[] byteWrite)
    throws IOException{
        int writeLen = byteWrite.length;
        int spaceNeeded = writeLen + SIZE_OF_SLOT;

        // Start by checking if sufficient space exists.
        // This is an upper bound check. May not actually need a slot
        // if we can find an empty one.
        
        freeSpace = Convert.getShortValue (FREE_SPACE, data);
        if (spaceNeeded > freeSpace) {
            return;
        
        } else {
        
        // look for an empty slot
        slotCnt = Convert.getShortValue (SLOT_CNT, data); 
        int i; 
        short length;
        for (i= 0; i < slotCnt; i++) 
        {
            length = getSlotLength(i); 
            if (length == EMPTY_SLOT)
                break;
        }
        
        if(i == slotCnt)   //use a new slot
        {           
            // adjust free space        
            freeSpace -= spaceNeeded;
            Convert.setShortValue (freeSpace, FREE_SPACE, data);
            
            slotCnt++;
            Convert.setShortValue (slotCnt, SLOT_CNT, data);
            
        }
        else {
        // reusing an existing slot
        freeSpace -= writeLen;
        Convert.setShortValue (freeSpace, FREE_SPACE, data);
        }
            
        usedPtr = Convert.getShortValue (USED_PTR, data);
            usedPtr -= writeLen;    // adjust usedPtr
        Convert.setShortValue (usedPtr, USED_PTR, data);
        
        //insert the slot info onto the data page
        setSlot(i, writeLen, usedPtr);   
        
        // insert data onto the data page
        System.arraycopy (byteWrite, 0, data, usedPtr, writeLen);
        curPage.pid = Convert.getIntValue (CUR_PAGE, data);
        return;
        }

        }
}
