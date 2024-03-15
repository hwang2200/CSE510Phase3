package columnar;

import global.AttrType;
import global.Convert;
import heap.*;
import TID.*;
import bitmap.BitMapFile;
import value.*;
import btree.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class Columnarfile {
    private String name;
    public static int numColumns;
    public AttrType[] type;
    public Heapfile[] heapfiles;

    public Columnarfile(String name, int numColumns, AttrType[] type)
            throws IOException, HFDiskMgrException, HFException, HFBufMgrException {
        this.name = name;
        Columnarfile.numColumns = numColumns;
        this.type = type;
        this.heapfiles = new Heapfile[numColumns];

        // Create a heapfile for each column
        for (int i = 0; i < numColumns; i++) {
            heapfiles[i] = new Heapfile(this.name + ".columnid" + i);
        }

        // Initialize the metadata file
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(this.name + ".hdr"))) {
            oos.writeObject(this.type); // Write the type array to the metadata file
        } catch (IOException e) {
            throw new IOException("Unable to create or write to the metadata file.", e);
        }

    }

    // Deletes all the column files and the metadata file associated with this
    // columnar file
    void deleteColumnarFile() throws IOException {
        for (int i = 0; i < numColumns; i++) {
            File columnFile = new File(this.name + ".columnid" + i);
            if (columnFile.exists()) {
                if (!columnFile.delete()) {
                    throw new IOException("Failed to delete column file: " + columnFile.getName());
                }
            }
        }

        File metadataFile = new File(this.name + ".hdr");
        if (metadataFile.exists() && !metadataFile.delete()) {
            throw new IOException("Failed to delete metadata file: " + metadataFile.getName());
        }
    }

    // Inserts a new tuple into the columnar file
    public TID insertTuple(byte[] tuplePtr) throws Exception {
        // Number of records in a tuple = number of columns
        TID tid = new TID(numColumns);
        tid.numRIDs = numColumns;
        int offset = 0;

        // For each type
        for (int i = 0; i < type.length; i++) {
            // Check attribute type and convert data accordingly
            if (type[i].attrType == AttrType.attrInteger) {
                int dataInt = Convert.getIntValue(offset, tuplePtr);
                byte[] newData = new byte[4];
                Convert.setIntValue(dataInt, offset, newData);
                offset = offset + 4;

                tid.recordIDs[i] = heapfiles[i].insertRecord(newData);
            }
            if (type[i].attrType == AttrType.attrString) {
                String dataStr = Convert.getStrValue(offset, tuplePtr, tuplePtr[i]);
                byte[] newData = new byte[3];
                Convert.setStrValue(dataStr, offset, newData);
                offset = offset + 3;

                tid.recordIDs[i] = heapfiles[i].insertRecord(newData);
            }
        }
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
        TID tid = new TID(numColumns);
        int keyType = 0;
        int keySize = 4;
        // int offset = 0;

        for (int i = 0; i < numColumns; i++) {
            keyType = type[i].attrType;
            // Tuple tuple = heapfiles[i].getRecord(tid.recordIDs[i]);
            // byte[] dataArray = tuple.returnTupleByteArray();
            // int dataInt = Convert.getIntValue(offset, dataArray);
            key = new IntegerKey(i);
            // KeyDataEntry pair = new KeyDataEntry(key, tid.recordIDs[i]);
            BTreeFile btreeFile = new BTreeFile("BTree File " + i, keyType, keySize, DeleteFashion.FULL_DELETE);

            btreeFile.insert(key, tid.recordIDs[i]);
        }
        return true;
    }

    public boolean createBitMapIndex(int columnNo, ValueClass value) {
        try {
            new BitMapFile(this.name, this, columnNo, value);
            
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
                int dataInt = Convert.getIntValue(offset, deletedTid);
                deletedTid = new byte[4];
                Convert.setIntValue(dataInt, offset, deletedTid);
                deletedTuple.tupleSet(deletedTid, offset, deletedTid.length);
                offset = offset + 4;

                if (!heapfiles[i].deleteRecord(tid.recordIDs[i]))
                    return false;
            } else if (type[i].attrType == AttrType.attrString) {
                tidTuple = heapfiles[i].getRecord(tid.recordIDs[i]);
                deletedTid = tidTuple.returnTupleByteArray();
                // String length?
                String dataString = Convert.getStrValue(offset, deletedTid, deletedTid[i]);
                deletedTid = new byte[dataString.length()];
                Convert.setStrValue(dataString, offset, deletedTid);
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
}
