package columnar;

import global.AttrType;
import global.Convert;
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
        this.heapfiles = new Heapfile[numColumns];

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
        //I don't know where split tuple is coming from - jaesang
        //Need to insert based on attribute type - jaesang
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

    public Tuple getTuple(TID tid) throws Exception {
        //new tuple must contain byte array which contains data, offset, and length of byte array
        Tuple result = new Tuple();
        byte[] resultData = new byte[0];
        int offset = 0;

        //For each column (since each column contains a heapfile)
        for(int i = 0; i < numColumns; i++)
        {
            //Get all the records in a heapfile associated with tid
            result = heapfiles[i].getRecord(tid.recordIDs[i]);
            byte[] data = result.returnTupleByteArray();

            //If type associated with that record is...
            //Write appropriate data to byte array
            if(type[i].attrType == AttrType.attrInteger)
            {
                int dataInt = Convert.getIntValue(offset, data);
                Convert.setIntValue(dataInt, offset, resultData);
                offset += 4;
                //Write data array associated with rid to new byte array

            }

            if(type[i].attrType == AttrType.attrString)
            {
                // Not sure how to get string length for conversion - jaesang
                String dataString = Convert.getStrValue(offset, data, data[i]);
                Convert.setStrValue(dataString, offset, resultData);
                offset += dataString.length();
            }

            if(type[i].attrType == AttrType.attrReal)
            {
                float dataFloat = Convert.getFloValue(offset, data);
                Convert.setFloValue(dataFloat, offset, resultData);
                offset += 4;
            }

            //Update tuple with resulting data
            result.tupleSet(resultData, 0, resultData.length);
        }
        return result;
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
