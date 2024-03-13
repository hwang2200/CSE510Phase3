import btree.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;
import java.io.*;

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
            // Define the attributes of the input fields
            AttrType[] attrTypes = new AttrType[targetColumnNames.length];
            short[] sSizes = new short[]{25,25}; // Assuming no string fields
            for (int i = 0; i < targetColumnNames.length; i++) {
                attrTypes[i] = new AttrType(AttrType.attrInteger); // Assuming all attributes are integers
            }

            // Define the output tuple fields
            FldSpec[] projList = new FldSpec[targetColumnNames.length];
            for (int i = 0; i < targetColumnNames.length; i++) {
                projList[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }

            // Define the condition expression
            CondExpr[] outFilter = new CondExpr[1];
            outFilter[0] = new CondExpr();

            // Parse the value constraint string and set the condition expression
            // Here, you need to parse the value constraint string to set the condition expression
            // For example, you may split the valueConstraint string to get the column name, operator, and value,
            // then set them in the outFilter array accordingly.

            // Create an instance of ColumnarFileScan
            ColumnarFileScan fileScan = new ColumnarFileScan(
                    columnarFileName, // Columnar file name
                    attrTypes, // Attribute types
                    sSizes, // String sizes
                    (short) attrTypes.length, // Number of attributes
                    targetColumnNames.length, // Number of output fields
                    projList, // Projection list
                    outFilter // Output filter
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

    /*private static void performColumnScan(String columnarFileName, String[] targetColumnNames, String valueConstraint) throws IndexException, InvalidTupleSizeException, IOException, UnknownIndexTypeException, InvalidTypeException {
        try {
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
            if(columnarFileName != null)
            {
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

                for(int i = 0; i < columns.length; i++)
                {
                    String[] columnDetails = columns[i].split(":");
                    columnNames[i] = columnDetails[0];

                    if(columnDetails[1].equals("int"))
                    {
                        columnTypes[i] = new AttrType(AttrType.attrInteger);
                    }
                    else
                    {
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


            //TODO: REMOVE THIS, ChatGPT help for valueConstraint
                /*CondExpr valueConstraintExpr = new CondExpr();

                // Set the operator
                valueConstraintExpr.op = new AttrOperator(AttrOperator.aopLT); // Assuming less than

                // Set the types of operands
                valueConstraintExpr.type1 = new AttrType(AttrType.attrSymbol);
                valueConstraintExpr.type2 = new AttrType(AttrType.attrInteger);

                // Set the left operand to be the target column
                valueConstraintExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), targetColumnIndex);

                // Set the right operand to be the value constraint
                valueConstraintExpr.operand2.integer = valueConstraint;

                // Set the next pointer to null since this is a single condition
                valueConstraintExpr.next = null;


            // Initialize ColumnarIndexScan object
            ColumnarIndexScan columnarIndexScan = new ColumnarIndexScan(
                    columnarFileName,
                    fldNums,
                    new IndexType[targetColumnNames.length],  // Assuming no indexes for now
                    new String[targetColumnNames.length],    // Assuming no indexes for now
                    attrTypes,
                    strSizes,
                    noInFlds,
                    noOutFlds,
                    outFlds,
                    valueConstraint,
                    false  // Assuming not index only for now
            );

            // Perform column scan
            Tuple tuple;
            while ((tuple = columnarIndexScan.get_next()) != null) {
                // Process each retrieved tuple
                // You can print it or do other operations as needed
                System.out.println(tuple);
            }

            // Close the column scan
            columnarIndexScan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/

    private static void performBTreeScan(String columnarFileName, String[] targetColumnNames, String valueConstraint) {
        try {
            // Open the B-tree index file
            BTreeFile btreeFile = new BTreeFile(columnarFileName);

            // Initialize the B-tree scan
            BTFileScan btreeScan = btreeFile.new_scan(null, null);

            // Iterate over the B-tree index entries
            KeyDataEntry entry;
            while ((entry = btreeScan.get_next()) != null) {
                // Extract key and data from the entry
                IntegerKey key = (IntegerKey) entry.key;
                RID rid = ((LeafData) entry.data).getData();

                // Process the entry according to your query requirements
                // For example, you can print the key and RID
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
