package columnar;

import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;
import TID.*;

import java.io.IOException;

public class TupleScan
{
    //Columnar file we are using
    private Columnarfile _cf;
    private Scan[] scan;

    //Constructor
    public TupleScan(Columnarfile cf) throws InvalidTupleSizeException, IOException {
        this._cf = cf;
        this.scan = new Scan[_cf.numColumns]; //Scanner for each column since each column has a heapfile

        //Initialize new scanner for each heapfile in columnarfile
        int i = 0;
        for(Heapfile hf : _cf.heapfiles)
        {
            scan[i] = hf.openScan();
            i++;
        }
    }

    //Methods
    public void closetuplescan()
    {
        for(Scan s : scan)
        {
            //Call scan closescan() function
            s.closescan();
        }
    }

    public Tuple getNext(TID tid) throws InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException {
        Tuple nextTID = new Tuple();
        for(Scan s : scan)
        {
            for(int i = 0; i < _cf.type.length; i++)
            {
                Tuple tuple = s.getNext(tid.recordIDs[i]);
                
                if(_cf.type[i].attrType == AttrType.attrInteger)
                {
                    nextTID.setIntFld(i, tuple.getIntFld(i));
                }
                if(_cf.type[i].attrType == AttrType.attrString)
                {
                    nextTID.setStrFld(i, tuple.getStrFld(i));
                }
                if(_cf.type[i].attrType == AttrType.attrReal)
                {
                    nextTID.setFloFld(i, tuple.getFloFld(i));
                }
            }
        }
        return nextTID;
    }

    public boolean position(TID tid) throws InvalidTupleSizeException, IOException
    {
        boolean result = true;
        int i = 0;
        for(Scan s : scan)
        {
            if(s.position(tid.recordIDs[i]) == false)
            {
                result = false;
                i++;
            }
        }
        return result;
    }
}
