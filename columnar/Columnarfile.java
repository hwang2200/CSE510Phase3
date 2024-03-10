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

    public ValueClass getValue(TID tid, int column) throws Exception
    {
        ValueClass result = null;
        IntegerValueClass integer = null;
        StringValueClass string = null;

        Tuple tuple = heapfiles[column].getRecord(tid.recordIDs[column]);
        byte[] data = tuple.returnTupleByteArray();
        int offset = 0;

        if(type[column].attrType == AttrType.attrInteger)
        {
            integer.setValue(Convert.getIntValue(offset, data));
            result = integer;
        }
        if(type[column].attrType == AttrType.attrInteger)
        {
            string.setValue(Convert.getStrValue(offset, data, data[0]));
            result = string;
        }

        return result;
    }

    public int getTupleCnt() throws HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, IOException {
        int count = 0;

        for(int i = 0; i < heapfiles.length; i++)
        {
            count += heapfiles[i].getRecCnt();
        }

        return count;
    }

    public TupleScan openTupleScan() throws InvalidTupleSizeException, IOException {
        TupleScan tupleScan = new TupleScan(this);
        return tupleScan;
    }

    public Scan openColumnScan(int columnNo) throws InvalidTupleSizeException, IOException {
        Scan scan = new Scan(heapfiles[columnNo]);
        return scan;
    }

    public boolean updateTuple(TID tid, heap.Tuple newtuple)
    {
        int i = 0;

		for (;i<numberOfColumns;i++) {
			if(!updateColumnofTuple(tid,newtuple,i+1))
				return false;
		}
		return true;
    }

    public boolean updateColumnofTuple(TID tid, heap.Tuple newtuple, int column)
    {
        int intValue;
		String strValue;
		Tuple tuple = null;
		try {
			if (attributeType[column-1].attrType == AttrType.attrInteger)	{
				intValue = newtuple.getIntFld(column);
				tuple = new Tuple(4);
				tuple.setIntFld(1, intValue);
			}
			else if (attributeType[column-1].attrType == AttrType.attrString)	{
				strValue = newtuple.getStrFld(column);
				tuple = new Tuple(stringSize);
				tuple.setStrFld(1, strValue);
			}

			return heapFileColumns[column-1].updateRecord(tid.recordIDs[column-1], tuple);

		}catch (Exception e)	{
			e.printStackTrace();
		}
		return false;
    }

    public boolean createBTreeIndex(int column)
    {
        return true;
    }

    public boolean createBitMapIndex(int columnNo, ValueClass value)
    {
        return true;
    }

    public boolean markTupleDeleted(TID tid)
    {
        byte[] deletedTids = new byte[numberOfColumns*4*2];

		int i = 0;
		int offset = 0;
		int tidOffset = 0;


		try{
			for (AttrType attr: attributeType) {
				if(attr.attrType == AttrType.attrInteger)
				{
					Convert.setIntValue(tid.recordIDs[i].pageNo.pid, tidOffset, deletedTids);
					Convert.setIntValue(tid.recordIDs[i].slotNo, tidOffset + 4, deletedTids);

					offset = offset + 4;
					tidOffset = tidOffset + 8;
					if(!heapFileColumns[i].deleteRecord(tid.recordIDs[i]))
						return false;
					i++;
				}
				else if(attr.attrType == AttrType.attrString)
				{
					Convert.setIntValue(tid.recordIDs[i].pageNo.pid, tidOffset, deletedTids);
					Convert.setIntValue(tid.recordIDs[i].slotNo, tidOffset + 4, deletedTids);

					offset = offset + stringSize;
					tidOffset = tidOffset + 8;
					if(!heapFileColumns[i].deleteRecord(tid.recordIDs[i]))
						return false;
					i++;
				}
			}
			deletedTupleList.insertRecord(deletedTids);
			this.deleteCount++;
			return true;
		}catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
    }

    public boolean purgeAllDeletedTuples()
    {
        return false;
    }
}
