package index;

import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;
import java.io.*;

public class ColumnIndexScan extends Iterator {

    private IndexFile indFile;
    private IndexFileScan indScan;
    private AttrType[] _types;
    private short[] _s_sizes;
    private CondExpr[] _selects;
    private int _noInFlds;
    private int _noOutFlds;
    private Heapfile f;
    private Tuple tuple1;
    private Tuple Jtuple;
    private int t1_size;
    private int _fldNum;
    private boolean index_only;

    public ColumnIndexScan(IndexType index,
                           final String relName,
                           final String indName,
                           AttrType type,
                           short str_sizes,
                           CondExpr[] selects,
                           boolean indexOnly)
            throws IndexException,
            InvalidTypeException,
            InvalidTupleSizeException,
            UnknownIndexTypeException,
            IOException {


        _fldNum = 1; // For ColumnIndexScan, we are accessing a single column, so always set _fldNum to 1
        _noInFlds = 1; // We are accessing a single column, so set _noInFlds to 1
        _types = new AttrType[]{type};
        _s_sizes = new short[]{str_sizes};
        _selects = selects;
        _noOutFlds = 1; // For ColumnIndexScan, always output a single field
        tuple1 = new Tuple();
        tuple1.setHdr((short) 1, _types, _s_sizes);
        t1_size = tuple1.size();
        index_only = indexOnly;

        try {
            f = new Heapfile(relName);
        } catch (HFException | HFBufMgrException | HFDiskMgrException e) {
            throw new RuntimeException(e);
        }

        switch (index.indexType) {
            case IndexType.B_Index:
                try {
                    indFile = new BTreeFile(indName);
                } catch (Exception e) {
                    throw new IndexException(e, "ColumnIndexScan: BTreeFile exceptions caught from BTreeFile constructor");
                }

                try {
                    indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
                } catch (Exception e) {
                    throw new IndexException(e, "ColumnIndexScan: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                }

                break;
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");
        }
    }

    public Tuple get_next()
            throws IndexException,
            UnknownKeyTypeException,
            IOException {
        RID rid;
        KeyDataEntry nextentry = null;

        try {
            nextentry = indScan.get_next();
        } catch (Exception e) {
            throw new IndexException(e, "ColumnIndexScan: BTree error");
        }

        while (nextentry != null) {
            if (index_only) {
                // only need to return the key
                Jtuple = new Tuple();
                AttrType[] attrType = new AttrType[1];
                short[] s_sizes = new short[1];

                if (_types[_fldNum - 1].attrType == AttrType.attrInteger) {
                    attrType[0] = new AttrType(AttrType.attrInteger);
                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    } catch (InvalidTypeException | InvalidTupleSizeException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        Jtuple.setIntFld(1, ((IntegerKey) nextentry.key).getKey());
                    } catch (FieldNumberOutOfBoundException e) {
                        throw new RuntimeException(e);
                    }
                } else if (_types[_fldNum - 1].attrType == AttrType.attrString) {
                    attrType[0] = new AttrType(AttrType.attrString);
                    int count = 0;
                    for (int i = 0; i < _fldNum; i++) {
                        if (_types[i].attrType == AttrType.attrString)
                            count++;
                    }
                    s_sizes[0] = _s_sizes[count - 1];
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

            // Not index_only, need to return the whole tuple
            rid = ((LeafData) nextentry.data).getData();
            try {
                tuple1 = f.getRecord(rid);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            try {
                tuple1.setHdr((short) _noInFlds, _types, _s_sizes);
            } catch (InvalidTypeException | InvalidTupleSizeException e) {
                throw new RuntimeException(e);
            }

            boolean eval = false;
            try {
                eval = PredEval.Eval(_selects, tuple1, null, _types, null);
            } catch (UnknowAttrType | InvalidTupleSizeException | InvalidTypeException |
                     FieldNumberOutOfBoundException | PredEvalException e) {
                throw new RuntimeException(e);
            }
            if (eval) {
                return tuple1;
            }

            try {
                nextentry = indScan.get_next();
            } catch (ScanIteratorException e) {
                throw new RuntimeException(e);
            }
        }

        return null;
    }

    public void close() throws IOException, IndexException {
        if (indScan instanceof BTFileScan) {
            try {
                ((BTFileScan) indScan).DestroyBTreeFileScan();
            } catch (Exception e) {
                throw new IndexException(e, "ColumnIndexScan: BTree error in destroying index scan.");
            }
        }
    }
}
