import btree.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;
import java.io.*;
import java.util.Objects;

public class QueryProgram {

    public static void main(String[] args) {
        if (args.length < 6) {
            System.err.println("Usage: java QueryProgram COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE");
            System.exit(1);
        }

        String columnDBName = args[0];
        String columnarFileName = args[1];
        String[] targetColumnNames = args[2].split(",");
        String valueConstraint = args[3];
        int numBuf = Integer.parseInt(args[4]);
        String accessType = args[5];

        try {

            ColumnDB testDB = new ColumnDB();
            testDB.openColumnDB(columnDBName);
            // Perform the query based on the access type
            switch (accessType.toUpperCase()) {
                case "FILESCAN":
                    performFileScan(columnarFileName, targetColumnNames, valueConstraint);
                    break;
                case "COLUMNSCAN":
                    performColumnScan(columnarFileName, targetColumnNames, valueConstraint);
                    break;
                case "BTREE":
                    performBTreeScan(columnarFileName, targetColumnNames, valueConstraint);
                    break;
                case "BITMAP":
                    performBitmapScan(columnarFileName, targetColumnNames, valueConstraint);
                    break;
                default:
                    System.err.println("Invalid access type");
                    break;
            }

            // Print out the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());

            testDB.closeDB();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void performFileScan(String columnarFileName, String[] targetColumnNames, String valueConstraint) {
        try {
            short[] sSizes = new short[]{25,25};
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(columnarFileName));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
            String[] columns = new String[0];
            try {
                columns = br.readLine().split(" ");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            //String[] columnNames = new String[targetColumnNames.length];
            AttrType[] attrTypes = new AttrType[targetColumnNames.length];

            for (int i = 0; i < columns.length; i++) {
                String[] columnDetails = columns[i].split(":");
                columns[i] = columnDetails[0];

                if(columnDetails[1].equals("int"))
                {
                    attrTypes[i] = new AttrType(AttrType.attrInteger);
                }
                else
                {
                    attrTypes[i] = new AttrType(AttrType.attrInteger);
                }
            }

            FldSpec[] projList = new FldSpec[targetColumnNames.length];
            for (int i = 0; i < targetColumnNames.length; i++) {
                projList[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }

            //obtains value constraints and ops
            CondExpr[] valueConstraintExpr = new CondExpr[1];
            String[] values = valueConstraint.split(" ");
            String columnName = values[0];
            int columnNum = 0;
            String operator = values[1];
            String value = values[2];

            if(Objects.equals(operator, "<"))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopLT);
            }
            else if(Objects.equals(operator, ">"))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopGT);
            }
            else if(Objects.equals(operator, "="))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopEQ);
            }
            else if(Objects.equals(operator, "!="))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopNE);
            }
            else if(Objects.equals(operator, "<="))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopLE);
            }
            else if(Objects.equals(operator, ">="))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopGE);
            }

            if(Objects.equals(columnName, "A"))
            {
                columnNum = 0;
            }
            else if(Objects.equals(columnName, "B"))
            {
                columnNum = 1;
            }
            else if(Objects.equals(columnName, "C")) {
                columnNum = 2;
            }
            else if(Objects.equals(columnName, "D"))
            {
                columnNum = 3;
            }

            valueConstraintExpr[0].type1 = new AttrType(AttrType.attrSymbol);
            valueConstraintExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), columnNum);

            if(value.matches("^\\d+$"))
            {
                valueConstraintExpr[0].type2 = new AttrType(AttrType.attrInteger);
                valueConstraintExpr[0].operand2.integer = Integer.parseInt(value);
            }
            else
            {
                valueConstraintExpr[0].type2 = new AttrType(AttrType.attrInteger);
                valueConstraintExpr[0].operand2.string = value;
            }

            ColumnarFileScan fileScan = new ColumnarFileScan(
                    columnarFileName,
                    attrTypes,
                    sSizes,
                    (short) attrTypes.length,
                    targetColumnNames.length,
                    projList,
                    valueConstraintExpr
            );

            // Get tuples one by one
            Tuple tuple;
            while ((tuple = fileScan.get_next()) != null) {
                // Process the tuple here
                System.out.println(tuple);
            }

            // Close the file scan
            fileScan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void performColumnScan(String columnarFileName, String[] targetColumnNames, String valueConstraint) throws IndexException, InvalidTupleSizeException, IOException, UnknownIndexTypeException, InvalidTypeException, UnknownKeyTypeException {
        // Initialize FldSpec[] for the output fields
        FldSpec[] outFlds = new FldSpec[targetColumnNames.length];
        for (int i = 0; i < targetColumnNames.length; i++) {
            outFlds[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }

        // Get the attribute types and sizes from the catalog or wherever it's stored
        AttrType[] attrTypes = new AttrType[targetColumnNames.length];
        short[] strSizes = new short[targetColumnNames.length];

        // Assuming you have a method to retrieve attribute types and sizes for the specified column names
        // You need to implement this method based on your catalog or metadata management
        if (columnarFileName != null) {
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(columnarFileName));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
            String[] columns = new String[0];
            try {
                columns = br.readLine().split(" ");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String[] columnNames = new String[targetColumnNames.length];
            AttrType[] columnTypes = new AttrType[targetColumnNames.length];

            for (int i = 0; i < columns.length; i++) {
                String[] columnDetails = columns[i].split(":");
                columnNames[i] = columnDetails[0];

                if (columnDetails[1].equals("int")) {
                    columnTypes[i] = new AttrType(AttrType.attrInteger);
                } else {
                    columnTypes[i] = new AttrType(AttrType.attrInteger);
                }
            }

            // Set up other parameters
            int noInFlds = attrTypes.length;
            int noOutFlds = outFlds.length;
            int[] fldNums = new int[targetColumnNames.length];
            for (int i = 0; i < targetColumnNames.length; i++) {
                fldNums[i] = i + 1;
            }

            CondExpr[] valueConstraintExpr = new CondExpr[1];
            String[] values = valueConstraint.split(" ");
            String columnName = values[0];
            int columnNum = 0;
            String operator = values[1];
            String value = values[2];

            if(Objects.equals(operator, "<"))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopLT);
            }
            else if(Objects.equals(operator, ">"))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopGT);
            }
            else if(Objects.equals(operator, "="))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopEQ);
            }
            else if(Objects.equals(operator, "!="))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopNE);
            }
            else if(Objects.equals(operator, "<="))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopLE);
            }
            else if(Objects.equals(operator, ">="))
            {
                valueConstraintExpr[0].op = new AttrOperator(AttrOperator.aopGE);
            }

            if(Objects.equals(columnName, "A"))
            {
                columnNum = 0;
            }
            else if(Objects.equals(columnName, "B"))
            {
                columnNum = 1;
            }
            else if(Objects.equals(columnName, "C")) {
                columnNum = 2;
            }
            else if(Objects.equals(columnName, "D"))
            {
                columnNum = 3;
            }

            valueConstraintExpr[0].type1 = new AttrType(AttrType.attrSymbol);
            valueConstraintExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), columnNum);

            if(value.matches("^\\d+$"))
            {
                valueConstraintExpr[0].type2 = new AttrType(AttrType.attrInteger);
                valueConstraintExpr[0].operand2.integer = Integer.parseInt(value);
            }
            else
            {
                valueConstraintExpr[0].type2 = new AttrType(AttrType.attrInteger);
                valueConstraintExpr[0].operand2.string = value;
            }

            ColumnarIndexScan columnarIndexScan = new ColumnarIndexScan(
                    columnarFileName,
                    fldNums,
                    new IndexType[targetColumnNames.length],
                    new String[targetColumnNames.length],
                    attrTypes,
                    strSizes,
                    noInFlds,
                    noOutFlds,
                    outFlds,
                    valueConstraintExpr,
                    false  // Assuming not index only for now
            );

            // Perform column scan
            Tuple tuple;
            while ((tuple = columnarIndexScan.get_next()) != null) {
                System.out.println(tuple);
            }

            columnarIndexScan.close();
        }
    }

        private static void performBTreeScan(String columnarFileName, String[] targetColumnNames, String valueConstraint) {
        try {
            BTreeFile btreeFile = new BTreeFile(columnarFileName);
            BTFileScan btreeScan = btreeFile.new_scan(null, null);

            // Iterate over the B-tree index entries
            KeyDataEntry entry;
            while ((entry = btreeScan.get_next()) != null) {
                IntegerKey key = (IntegerKey) entry.key;
                RID rid = ((LeafData) entry.data).getData();

                System.out.println("Key: " + key.getKey() + ", RID: " + rid.toString());
            }

            // Close the B-tree scan
            btreeScan.DestroyBTreeFileScan();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void performBitmapScan(String columnarFileName, String[] targetColumnNames, String valueConstraint) {
        // Implement bitmap scan query
    }
}
