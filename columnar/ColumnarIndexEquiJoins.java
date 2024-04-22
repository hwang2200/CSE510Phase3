package columnar;

import bitmap.BMPage;
import bitmap.BitMapFile;
import bitmap.BitMapHeaderPage;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import btree.UnpinPageException;
import diskmgr.Page;
import global.AttrType;
import global.IndexType;
import global.PageId;
import global.RID;
import heap.*;
import iterator.FldSpec;

import java.io.IOException;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Enumeration;

import static global.GlobalConst.INVALID_PAGE;

public class ColumnarIndexEquiJoins {

    int cf2startpid;
    BitMapFile combinedbmfile;
    String combinedBMFname;
    public ColumnarIndexEquiJoins(

        Columnarfile cf1,
        Columnarfile cf2,
        int amt_of_mem,
        int leftJoinField,
        int rightJoinField,
        IndexType[] rightIndex,
        FldSpec[] proj_list,
        int n_out_flds) throws HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, IOException {

        System.out.println("RecCnt for Left: " + cf1.heapfiles[leftJoinField].getRecCnt());
        System.out.println("RecCnt for Right: " + cf2.heapfiles[rightJoinField].getRecCnt());


        BitMapFile bmf1 = cf1.BMFiles[leftJoinField];
        BitMapFile bmf2 = cf2.BMFiles[rightJoinField];

        boolean compressed;
        if (rightIndex[0].indexType == IndexType.BitMapIndex) { compressed = false;}
        else { compressed = true; }

        PageId p1 = bmf1.getHeaderPage().get_rootId();
        Page pg1 = null;
        try
        {
            pg1 = bmf1.pinPage(p1);
        } catch (PinPageException e) {
            throw new RuntimeException(e);
        }
        assert pg1 != null;
        BMPage page1 = new BMPage(pg1);
        cf2startpid = page1.getCurPage().pid + 1;

        try
        {
            bmf1.unpinPage(p1);
        }catch (UnpinPageException e) {
            throw new RuntimeException(e);
        }

        PageId p2 = bmf2.getHeaderPage().get_rootId();
        Page pg2 = null;
        try
        {
            System.out.println("Pinning:" + p2.pid);
            pg2 = bmf2.pinPage(p2);
        } catch (PinPageException e) {
            throw new RuntimeException(e);
        }
        assert pg2 != null;



        do{
            BMPage page2 = new BMPage(pg2);

            RID tmpRID = new RID();
            try{
                tmpRID = page2.firstRecord();
            }catch (IOException e) {
                throw new RuntimeException(e);
            }

            Tuple t = new Tuple();

            do {
                try {
                    t = page2.getRecord(tmpRID);
                }catch (IOException | InvalidSlotNumberException e) {
                    throw new RuntimeException(e);
                }

                byte[] data = t.getTupleByteArray();
                if(!compressed)
                {
                    for(int i = 0; i < data.length; i++)
                    {
                        if(data[i] == 1)
                        {
                            bmf1.Insert(i, data.length, false);
                            break;
                        }
                    }
                }
                else {
                    bmf1.Insert(data[0], cf2.intBitmapRange, true);
                }

                try{
                    tmpRID = page2.nextRecord(tmpRID);
                }catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }while(tmpRID != null);

            System.out.println("Unpinning:" + page2.getCurPage());
            try {
                if(page2.getCurPage().pid == INVALID_PAGE) {break;}
                bmf2.unpinPage(page2.getCurPage());
            }catch (Exception e)
            {
                throw new RuntimeException(e);
            }



            try {
                page2.setCurPage(page2.getNextPage());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try {
                if(page2.getCurPage().pid == INVALID_PAGE) { break; }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


            System.out.println("Pinning:" + page2.getCurPage());
            try {

                pg2 = bmf2.pinPage(page2.getCurPage());
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }while(true);

        combinedbmfile = bmf1;
        combinedBMFname = combinedbmfile.bmfilename;


    }

    @Override
    public String toString()
    {
        int count = 0;
        BitMapFile tmpBMF = null;

        System.out.println(combinedBMFname);
        tmpBMF = this.combinedbmfile;

        PageId firstPage = new PageId(tmpBMF.getHeaderPage().firstPID + 1);
        Page pg1 = null;
        try
        {
            pg1 = tmpBMF.pinPage(firstPage);
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
                if(bmpage.getCurPage().pid == INVALID_PAGE) {break;}
                tmpBMF.unpinPage(bmpage.getCurPage());
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
                pg1 = tmpBMF.pinPage(bmpage.getCurPage());
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }while(true);


        return "";
    }

}



