package index;

import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;
import java.io.*;

public class ColumnarIndexScan extends Iterator {

    private IndexFile[] indFiles;
    private IndexFileScan[] indScans;
    private AttrType[] types;
    private short[] str_sizes;
    private CondExpr[] selects;
    private int noInFlds;
    private int noOutFlds;
    private Heapfile f;
    private Tuple tuple1;
    private Tuple Jtuple;
    private int[] fldNums;
    private boolean indexOnly;

    public ColumnarIndexScan(String relName,
                             int[] fldNums,
                             IndexType[] indexTypes,
                             String[] indNames,
                             AttrType[] types,
                             short[] str_sizes,
                             int noInFlds,
                             int noOutFlds,
                             FldSpec[] outFlds,
                             CondExpr[] selects,
                             boolean indexOnly)
            throws IndexException,
            InvalidTypeException,
            InvalidTupleSizeException,
            UnknownIndexTypeException,
            IOException {

        this.fldNums = fldNums;
        this.noInFlds = noInFlds;
        this.types = types;
        this.str_sizes = str_sizes;
        this.selects = selects;
        this.noOutFlds = noOutFlds;
        this.indexOnly = indexOnly;

        try {
            f = new Heapfile(relName);
        } catch (HFException | HFBufMgrException | HFDiskMgrException e) {
            throw new RuntimeException(e);
        }
        indFiles = new IndexFile[indNames.length];
        indScans = new IndexFileScan[indNames.length];

        for (int i = 0; i < indNames.length; i++) {
            switch (indexTypes[i].indexType) {
                case IndexType.B_Index:
                    try {
                        indFiles[i] = new BTreeFile(indNames[i]);
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnarIndexScan: BTreeFile exceptions caught from BTreeFile constructor");
                    }

                    try {
                        indScans[i] = (BTFileScan) IndexUtils.BTree_scan(selects, indFiles[i]);
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnarIndexScan: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                    }
                    break;
                default:
                    throw new UnknownIndexTypeException("Only BTree index is supported so far");
            }
        }
    }

    public Tuple get_next()
            throws IndexException,
            UnknownKeyTypeException,
            IOException {
        RID rid;
        KeyDataEntry nextentry = null;

        try {
            for (IndexFileScan indScan : indScans) {
                nextentry = indScan.get_next();
                if (nextentry != null)
                    break; // Exit loop as soon as one entry is found
            }
        } catch (Exception e) {
            throw new IndexException(e, "ColumnarIndexScan: Error in getting next entry from index.");
        }

        while (nextentry != null) {
            if (indexOnly) {
                // Only need to return the key
                Jtuple = new Tuple();
                AttrType[] attrType = new AttrType[1];
                short[] s_sizes = new short[1];

                int fldNum = fldNums[0] - 1; // Adjust field number for 1-based indexing
                if (types[fldNum].attrType == AttrType.attrInteger) {
                    attrType[0] = new AttrType(AttrType.attrInteger);
                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    } catch (InvalidTypeException | InvalidTupleSizeException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        Jtuple.setIntFld(1, ((IntegerKey) nextentry.key).getKey().intValue());
                    } catch (FieldNumberOutOfBoundException e) {
                        throw new RuntimeException(e);
                    }
                } else if (types[fldNum].attrType == AttrType.attrString) {
                    attrType[0] = new AttrType(AttrType.attrString);
                    int count = 0;
                    for (int i = 0; i < fldNum; i++) {
                        if (types[i].attrType == AttrType.attrString)
                            count++;
                    }
                    s_sizes[0] = str_sizes[count];
                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    } catch (InvalidTypeException | InvalidTupleSizeException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        Jtuple.setStrFld(1, ((StringKey) nextentry.key).getKey());
                    } catch (FieldNumberOutOfBoundException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
                }
                return Jtuple;
            }

            // Not indexOnly, need to return the whole tuple
            rid = ((LeafData) nextentry.data).getData();
            try {
                tuple1 = f.getRecord(rid);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            try {
                tuple1.setHdr((short) noInFlds, types, str_sizes);
            } catch (InvalidTypeException | InvalidTupleSizeException e) {
                throw new RuntimeException(e);
            }

            boolean eval = false;
            try {
                eval = PredEval.Eval(selects, tuple1, null, types, null);
            } catch (UnknowAttrType | InvalidTupleSizeException | InvalidTypeException |
                     FieldNumberOutOfBoundException | PredEvalException e) {
                throw new RuntimeException(e);
            }
            if (eval) {
                return tuple1;
            }

            // Get the next entry from all index scans
            try {
                for (IndexFileScan indScan : indScans) {
                    nextentry = indScan.get_next();
                    if (nextentry != null)
                        break; // Exit loop as soon as one entry is found
                }
            } catch (Exception e) {
                throw new IndexException(e, "ColumnarIndexScan: Error in getting next entry from index.");
            }
        }

        return null;
    }

    public void close() throws IOException, IndexException {
        for (IndexFileScan indScan : indScans) {
            try {
                ((BTFileScan) indScan).DestroyBTreeFileScan();
            } catch (Exception e) {
                throw new IndexException(e, "ColumnarIndexScan: BTree error in destroying index scan.");
            }
        }
    }
}
