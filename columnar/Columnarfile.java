package columnar;

import bitmap.BM;
import diskmgr.*;
import global.*;
import heap.*;
import TID.*;
import bitmap.BitMapFile;
import value.*;
import btree.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class Columnarfile {
    public String name;
    public String fileName;
    public static int numColumns;
    public AttrType[] type;
    public Heapfile[] heapfiles;
    public BitMapFile[] BMFiles;
    public String[] heapFileNames;
    public Heapfile columnarFile;
    public int tupleLength;
    public short[] tupleOffSets;
    public String[] columnNames;
    public ColumnarFileMetadata columnarFileMetadata;
    public int intBitmapRange;
    public int strBitmapRange;

    public BTreeFile[] bTreeFiles;
    public Map<String, Integer> stringHashMap;

    public Columnarfile(String name, String[] colNames, int numColumns, AttrType[] type)
            throws IOException, HFDiskMgrException, HFException, HFBufMgrException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidTupleSizeException {
        this.name = name;
        Columnarfile.numColumns = numColumns;
        this.type = type;
        this.heapfiles = new Heapfile[numColumns];
        this.heapFileNames = new String[numColumns];
        this.BMFiles = new BitMapFile[numColumns];
        this.columnNames = colNames;
        this.bTreeFiles = new BTreeFile[columnNames.length];
        // Create a heapfile for each column
        for (int i = 0; i < numColumns; i++) {
            String heapName = this.name + "COL.columnid" + i;
            heapfiles[i] = new Heapfile(heapName);
            heapFileNames[i] = heapName;
        }

        // Initialize the metadata file
        columnarFileMetadata = new ColumnarFileMetadata(this);
        columnarFile = new Heapfile(this.name + ".hdr");
        columnarFileMetadata.columnarFileName = name;
        setColumnarFileMetadata();
        fileName = name + "COL";
    }

    // Deletes all the column files and the metadata file associated with this
    // columnar file
    void deleteColumnarFile() throws IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, FileAlreadyDeletedException {
        for (int i = 0; i < heapfiles.length; i++) {
            heapfiles[i].deleteFile();
        }

        //Delete metadata file associated with columnar file

    }

    // Inserts a new tuple into the columnar file
    public TID insertTuple(byte[] tuplePtr) throws Exception {
        // Number of records in a tuple = number of columns
        TID tid = new TID(numColumns);
        tid.numRIDs = numColumns;
        tid.recordIDs = new RID[numColumns];
        int offset = 0;

        int count = 0;
        // For each type
        for (int i = 0; i < type.length; i++) {
            // Check attribute type and convert data accordingly
            if (type[i].attrType == AttrType.attrInteger) {
                int dataInt = Convert.getIntValue(offset, tuplePtr);
                byte[] newData = new byte[4];
                Convert.setIntValue(dataInt, 0, newData);
                offset = offset + 4;

                tid.recordIDs[i] = new RID();
                tid.recordIDs[i] = heapfiles[i].insertRecord(newData);
            }
            if (type[i].attrType == AttrType.attrString) {
                String dataStr = Convert.getStrValue(offset, tuplePtr, 25);
                byte[] newData = new byte[25];
                Convert.setStrValue(dataStr, 0, newData);
                offset = offset + 25;

                tid.recordIDs[i] = new RID();
                tid.recordIDs[i] = heapfiles[i].insertRecord(newData);
            }
            count++;
        }
        tid.numRIDs = count;
        return tid;
    }

    public Tuple getTuple(TID tid) throws Exception {
        // new tuple must contain byte array which contains data, offset, and length of
        // byte array
        Tuple result = new Tuple();
        byte[] resultData = new byte[numColumns];
        int offset = 0;

        // For each column (since each column contains a heapfile)
        for (int i = 0; i < numColumns; i++) {
            // Get all the records in a heapfile associated with tid
            result = heapfiles[i].getRecord(tid.recordIDs[i]);
            byte[] data = result.returnTupleByteArray();

            // If type associated with that record is...
            // Write appropriate data to byte array
            if (type[i].attrType == AttrType.attrInteger) {
                int dataInt = Convert.getIntValue(offset, data);
                resultData = new byte[4];
                Convert.setIntValue(dataInt, offset, resultData);
                offset += 4;

            }
            if (type[i].attrType == AttrType.attrString) {
                // Not sure how to get string length for conversion - jaesan
                String dataString = Convert.getStrValue(offset, data, data[i]);
                resultData = new byte[data[i]];
                Convert.setStrValue(dataString, offset, resultData);
                offset += dataString.length();
            }

            // Update tuple with resulting data
            result.tupleSet(resultData, 0, resultData.length);
        }
        return result;
    }

    public ValueClass getValue(TID tid, int column) throws Exception {
        ValueClass result = null;
        IntegerValueClass integer = new IntegerValueClass();
        StringValueClass string = new StringValueClass();

        Tuple tuple = heapfiles[column].getRecord(tid.recordIDs[column]);
        byte[] data = tuple.returnTupleByteArray();
        int offset = 0;

        if (type[column].attrType == AttrType.attrInteger) {
            integer.setValue(Convert.getIntValue(offset, data));
            result = integer;
        }
        if (type[column].attrType == AttrType.attrInteger) {
            string.setValue(Convert.getStrValue(offset, data, data[0]));
            result = string;
        }

        return result;
    }

    public int getTupleCnt() throws HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException,
            HFBufMgrException, IOException {
        return heapfiles[0].getRecCnt();
    }

    public TupleScan openTupleScan() throws InvalidTupleSizeException, IOException {
        TupleScan tupleScan = new TupleScan(this);
        return tupleScan;
    }

    public Scan openColumnScan(int columnNo) throws InvalidTupleSizeException, IOException {
        Scan scan = new Scan(heapfiles[columnNo]);
        return scan;
    }

    public boolean updateTuple(TID tid, Tuple newtuple) throws Exception {
        for (int i = 0; i < numColumns; i++) {
            if (!updateColumnofTuple(tid, newtuple, i))
                return false;
        }
        return true;
    }

    public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column) throws Exception {
        boolean result = false;

        if (type[column].attrType == AttrType.attrInteger) {
            int dataInt = newtuple.getIntFld(column);
            Tuple tuple = new Tuple(4);
            tuple.setIntFld(column, dataInt);
            result = heapfiles[column].updateRecord(tid.recordIDs[column], tuple);

        } else if (type[column].attrType == AttrType.attrString) {
            String dataStr = newtuple.getStrFld(column);
            Tuple tuple = new Tuple(dataStr.length());
            tuple.setStrFld(column, dataStr);
            result = heapfiles[column].updateRecord(tid.recordIDs[column], tuple);
        }

        return result;
    }

    public boolean createBTreeIndex(int column) throws Exception {
        KeyClass key = null;
        //TID tid = new TID(numColumns);
        //tid.numRIDs = numColumns;
        //tid.recordIDs = new RID[numColumns];
        //System.out.println(heapfiles[column]);

        int keyType = 0;
        int keySize = 4;

        keyType = type[column].attrType;
        if(type[column].attrType == AttrType.attrInteger)
        {
            key = new IntegerKey(column);
        }
        else{
            key = new StringKey(columnNames[column]);
            keySize = 25;
        }

        BTreeFile btreeFile = new BTreeFile("BTreeFile.col" + column, keyType, keySize, DeleteFashion.FULL_DELETE);
        this.bTreeFiles[column] = btreeFile;

        RID rid = new RID();
        Scan s = heapfiles[column].openScan();
        Tuple tuple = s.getNext(rid); // tuple is also null, probably bc the rid is not set but

        while(tuple != null)
        {
            btreeFile.insert(key, rid);

            //System.out.println("Tuple created! RID: " + rid);
            tuple = s.getNext(rid);
        }

        //TODO:DELETE
            try {
                BTFileScan btscan = btreeFile.new_scan(null, null);

                KeyDataEntry tmpKDE = btscan.get_next();
                while (tmpKDE != null) {
                    LeafData tmpData = (LeafData) tmpKDE.data;
                    RID tmpRid = tmpData.getData();
                    tmpKDE = btscan.get_next();
                }

            }
            catch (Exception e)
            {
                System.out.println("Error occurred while doing BTScan");
                return false;
            }

        return true;
    }

    public boolean createBitMapIndex(int columnNo, ValueClass value) {
        try {
            BitMapFile bitmapFile;
            String filename = this.name + "_" + columnNo;

            if(SystemDefs.JavabaseDB.get_file_entry(filename) != null)
            {
                bitmapFile = new BitMapFile(filename);
            }
            else
            {
                bitmapFile = new BitMapFile(filename, this, columnNo, value);

            }

            if(value instanceof IntegerValueClass)
            {
                bitmapFile.Insert(((IntegerValueClass) value).getValue(), intBitmapRange);
            }
            else if(value instanceof StringValueClass)
            {
                int position = 0;
                RID rid = new RID();
                Scan s = heapfiles[columnNo].openScan();
                Tuple tuple = s.getNext(rid);

                while(tuple != null)
                {
                    byte[] strByteArray = tuple.returnTupleByteArray();
                    String strData = Convert.getStrValue(0, strByteArray, 25);
                    //Find which string it is in the dictionary
                    if(strData.equals(((StringValueClass) value).getValue()))
                    {
                        position = stringHashMap.get(strData);
                        break;
                    }
                    tuple = s.getNext(rid);
                }
                //Not position, but the identifier associated with each string
                bitmapFile.Insert(position, strBitmapRange);
            }



            //TODO
            //BTreeFile tmpB = new BTreeFile(filename);
            this.BMFiles[columnNo] = bitmapFile;
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean markTupleDeleted(TID tid) throws Exception {
        Heapfile deletedTuples = new Heapfile("Deleted Tuples");
        Tuple tidTuple = new Tuple();
        Tuple deletedTuple = new Tuple();
        byte[] deletedTid = new byte[numColumns];
        int offset = 0;

        // Delete all records associate with that tid and add those tuples to a heapfile
        for (int i = 0; i < type.length; i++) {
            if (type[i].attrType == AttrType.attrInteger) {
                tidTuple = heapfiles[i].getRecord(tid.recordIDs[i]);
                deletedTid = tidTuple.returnTupleByteArray();
                deletedTuple.tupleSet(deletedTid, offset, deletedTid.length);
                offset = offset + 4;

                if (!heapfiles[i].deleteRecord(tid.recordIDs[i]))
                    return false;
            } else if (type[i].attrType == AttrType.attrString) {
                tidTuple = heapfiles[i].getRecord(tid.recordIDs[i]);
                deletedTid = tidTuple.returnTupleByteArray();
                // String length?
                String dataString = Convert.getStrValue(offset, deletedTid, deletedTid[i]);
                //deletedTid = new byte[dataString.length()];
                //Convert.setStrValue(dataString, offset, deletedTid);
                deletedTuple.tupleSet(deletedTid, offset, deletedTid.length);
                offset = offset + dataString.length();

                if (!heapfiles[i].deleteRecord(tid.recordIDs[i]))
                    return false;
            }
        }
        deletedTuples.insertRecord(deletedTid);
        return true;
    }

    public boolean purgeAllDeletedTuples() throws HFDiskMgrException, HFException, HFBufMgrException, IOException,
            InvalidSlotNumberException, InvalidTupleSizeException, FileAlreadyDeletedException {
        Heapfile deletedTuples = new Heapfile("Deleted Tuples");
        deletedTuples.deleteFile();
        return true;
    }

    public void setColumnarFileMetadata() throws SpaceNotAvailableException, HFDiskMgrException, HFException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, IOException {
        for(int i = 0; i < numColumns; i++)
        {
            columnarFileMetadata.attributeType[i] = type[i].attrType;
            columnarFileMetadata.columnNames[i] = columnNames[i];
        }
        columnarFileMetadata.getTuple();
        columnarFile.insertRecord(columnarFileMetadata.data);
    }

    public ColumnarFileMetadata getColumnarFileMetadata()
    {
        return this.columnarFileMetadata;
    }

    public Heapfile getHeapfile(String columnName)
    {
        for(int i = 0; i < columnNames.length; i++)
        {
            if(columnNames[i].equalsIgnoreCase(columnName))
            {
                return heapfiles[i];
            }
        }
        return null;
    }

    @Override
    public String toString()
    {
        String result = "";
        result += ("ColumnarFileName: " + this.name +"\n");
        result += ("ColumnNums: " + numColumns + "\n");
        for(int i = 0; i < numColumns; i++)
        {
            try {
                result += columnNames[i] + ": " + heapfiles[i].getRecCnt();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return result;
    }
}
