package columnar;

import global.AttrType;
import global.RID;
import heap.*;
import TID.*;
import value.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class Columnarfile
{
    private String name;
    public static int numColumns;
    public AttrType[] type;
    public Heapfile[] heapfiles;

    public Columnarfile(String name, int numColumns, AttrType[] type) throws IOException, HFDiskMgrException, HFException, HFBufMgrException {
        this.name = name;
        Columnarfile.numColumns = numColumns;
        this.type = type;
        this.heapfiles = new heap.Heapfile[numColumns];

        // Create a heapfile for each column
        for (int i = 0; i < numColumns; i++)
        {
            heapfiles[i] = new Heapfile(this.name + ".columnid" + i);
        }

        // Initialize the metadata file
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(this.name + ".hdr")))
        {
            oos.writeObject(this.type); // Write the type array to the metadata file
        }
        catch (IOException e)
        {
            throw new IOException("Unable to create or write to the metadata file.", e);
        }

    }

    // Deletes all the column files and the metadata file associated with this columnar file
    void deleteColumnarFile() throws IOException {
        for (int i = 0; i < numColumns; i++) {
            File columnFile = new File(this.name + ".columnid" + i);
            if (columnFile.exists())
            {
                if (!columnFile.delete())
                {
                    throw new IOException("Failed to delete column file: " + columnFile.getName());
                }
            }
        }

        File metadataFile = new File(this.name + ".hdr");
        if (metadataFile.exists() && !metadataFile.delete())
        {
            throw new IOException("Failed to delete metadata file: " + metadataFile.getName());
        }
    }

    // Inserts a new tuple into the columnar file
    public TID insertTuple(byte[] tuplePtr) throws Exception {
        byte[][] columnValues = splitTuple(tuplePtr, this.type);
        TID tid = new TID(numColumns);
        for (int i = 0; i < numColumns; i++)
        {
            RID rid = heapfiles[i].insertRecord(columnValues[i]);
            if (rid == null)
            {
                throw new Exception("Insertion failed for column " + i);
            }
            tid.recordIDs[i] = rid;
        }
        return tid;

    }

    public heap.Tuple getTuple(TID tid)
    {
        return null;
    }

    public ValueClass getValue(TID tid, int column)
    {
        return null;
    }

    public int getTupleCnt()
    {
        return 0;
    }

    public TupleScan openTupleScan()
    {
        return null;
    }

    public heap.Scan openColumnScan(int columnNo)
    {
        return null;
    }

    public boolean updateTuple(TID tid, heap.Tuple newtuple)
    {
        return false;
    }

    public boolean updateColumnofTuple(TID tid, heap.Tuple newtuple, int column)
    {
        return false;
    }

    public boolean createBTreeIndex(int column)
    {
        return false;
    }

    public boolean createBitMapIndex(int columnNo, ValueClass value)
    {
        return false;
    }

    public boolean markTupleDeleted(TID tid)
    {
        return false;
    }

    public boolean purgeAllDeletedTuples()
    {
        return false;
    }
}
