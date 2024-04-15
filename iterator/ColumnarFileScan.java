package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import java.io.*;

public class ColumnarFileScan extends Iterator {
    private AttrType[] _in1;
    private short in1_len;
    private short[] s_sizes;
    private Heapfile f;
    private Scan scan;
    private Tuple tuple1;
    private Tuple Jtuple;
    private int t1_size;
    private int nOutFlds;
    private CondExpr[] OutputFilter;
    private FldSpec[] perm_mat;

    public ColumnarFileScan(String file_name,
                            AttrType[] in1,
                            short[] s1_sizes,
                            short len_in1,
                            int n_out_flds,
                            FldSpec[] proj_list,
                            CondExpr[] outFilter)
            throws IOException, FileScanException, TupleUtilsException, InvalidRelation {

        _in1 = in1;
        in1_len = len_in1;
        s_sizes = s1_sizes;
        Jtuple = new Tuple();
        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[] ts_size;
        ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);
        OutputFilter = outFilter;
        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        tuple1 = new Tuple();

        try {
            tuple1.setHdr(in1_len, _in1, s1_sizes);
        } catch (Exception e) {
            throw new FileScanException(e, "setHdr() failed");
        }
        t1_size = tuple1.size();
        try {
            f = new Heapfile(file_name);
        } catch (Exception e) {
            throw new FileScanException(e, "Create new heapfile failed");
        }
        try {
            scan = f.openScan();
        } catch (Exception e) {
            throw new FileScanException(e, "openScan() failed");
        }
    }

    public FldSpec[] show() {
        return perm_mat;
    }

    public Tuple get_next()
            throws IOException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            UnknowAttrType,
            FieldNumberOutOfBoundException,
            WrongPermat
    {
        RID rid = new RID();
        int count = 0;
        while (true) {
            if ((tuple1 = scan.getNext(rid)) == null) {
                System.out.println("Tuple count of scan: " + count);
                return null;
            }
            count++;
            tuple1.setHdr(in1_len, _in1, s_sizes);
            if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null)) {
                Projection.Project(tuple1, _in1, Jtuple, perm_mat, nOutFlds);
                return Jtuple;
            }
        }
    }

    public void close() {
        if (!closeFlag) {
            scan.closescan();
            closeFlag = true;
        }
    }
}
