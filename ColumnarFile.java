package Project2;

import global.AttrType;
import heap.Tuple;
import heap.Scan;

import java.lang.String;

public class ColumnarFile {
    //Fields
    static int numColumns;
    AttrType[] type;

    //Constructor
    public ColumnarFile(String name, int numColumns, AttrType[] type)
    {

    }

    //Methods
    public void deleteColumnarFile()
    {

    }

    public TID insertTuple(byte[] tuplePtr)
    {

    }

    public Tuple getTuple(TID tid)
    {

    }

    public ValueClass getValue(TID tid, int column)
    {

    }

    public int getTupleCnt()
    {

    }

    public TupleScan openTupleScan()
    {

    }

    public Scan openColumnScan(int columnNo)
    {

    }

    public boolean updateTuple(TID tid, Tuple newtuple)
    {

    }

    public boolean updateColumnOfTuple(TID tid, Tuple newtuple, int column)
    {

    }

    public boolean createBTreeIndex(int column)
    {

    }

    public boolean createBitMapIndex(int columnNo, ValueClass value)
    {

    }

    public boolean markTupleDeleted(TID tid)
    {

    }

    public boolean purgeAllDeletedTuples()
    {

    }
}
