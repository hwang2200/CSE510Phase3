package programs;

import TID.TID;
import bitmap.BM;
import bitmap.BMPage;
import bitmap.BitMapFile;
import bitmap.BitMapHeaderPage;
import btree.*;
import bufmgr.PageNotReadException;
import columnar.ColumnarFileMetadata;
import columnar.Columnarfile;
import columnar.TupleScan;
import index.ColumnarIndexScan;
import diskmgr.*;
import global.*;
import heap.*;
import iterator.*;
import org.w3c.dom.Attr;
import value.IntegerValueClass;
import value.StringValueClass;
import value.ValueClass;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import index.IndexException;
import index.UnknownIndexTypeException;
import java.util.*;

import static global.GlobalConst.INVALID_PAGE;


public class allPrograms {


    public static void main(String[] args) throws UnknownKeyTypeException, InvalidTupleSizeException, InvalidTypeException {
        Columnarfile columnarFile = null;
        BitMapFile bitmapFile = null;
        Scanner scanner = new Scanner(System.in);
        String option = null;
        String dataFileName;
        String colDBName;
        String columnarFileName;
        String numColumns;
        String columnName;
        String indexType;
        String[] queryArgs;
        String valueConstraint;
        String numBuf;
        String accessType;

        do {

            System.out.println("1. Batch Insert Program");
            System.out.println("2. Index Program");
            System.out.println("3. Query Program");
            System.out.println("4. Delete Query Program");
            System.out.println("5. Quit Program");
            System.out.println();
            System.out.print("Enter Option from above: ");
            option = scanner.nextLine();


            switch (option) {
                case "1":
                    //Scanner scan1 = new Scanner(System.in);
                    System.out.println("Welcome to batchinsert!");
                    //System.out.println("Please enter in a query in the format: DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");

                    System.out.println("Please enter in the name of the Data File: ");
                    dataFileName = "sd3"; //"sd2";//scanner.nextLine();

                    System.out.println("Please enter in the name of the Column DB: ");
                    colDBName = "sd2";//scanner.nextLine();

                    System.out.println("Please enter in the name of the Columnar File: ");
                    columnarFileName = "sd2";//scanner.nextLine();

                    System.out.println("Please enter in the number of columns: ");
                    numColumns = "4"; //scanner.nextLine();

                    queryArgs = new String[]{dataFileName, colDBName, columnarFileName, numColumns};

                    columnarFile = batchInsert(queryArgs);
                    //System.out.println(columnarFile.toString());

                    //scan1.close();
                    break;
                case "2":
                    //column DB name = specified above
                    //columnarfile name = returned by batchinsert

                    System.out.println("Welcome to index!");
                    //System.out.println("Please enter in a query in the format: COLUMNDBNAME COLUMNARFILENAME COLUMNAME INDEXTYPE");

                    System.out.println("Please enter in the name of the Column DB: ");
                    colDBName = "sd2DB"; //scanner.nextLine();

                    System.out.println("Please enter in the name of the Columnar File: ");
                    columnarFileName = "sd2COL"; //scanner.nextLine();

                    System.out.println("Please enter in the Target Column Name: ");
                    columnName = scanner.nextLine();

                    System.out.println("Please enter in the Index Type (\"BITMAP\", or \"CBITMAP\"): ");
                    indexType = scanner.nextLine();

                    queryArgs = new String[]{colDBName, columnarFileName, columnName, indexType};
                    index(queryArgs, columnarFile);

                    //scan2.close();
                    break;

                case "3":
                    System.out.println("Welcome to Query!");

                    System.out.println("Please enter in the name of the Column DB: ");
                    colDBName = "sd2DB"; //scanner.nextLine();

                    System.out.println("Please enter in the name of the Columnar File: ");
                    columnarFileName = "sd2COL"; //scanner.nextLine();

                    System.out.println("Please enter in the Target Column Name(s) separated by ',': ");
                    columnName = "C"; //scanner.nextLine();

                    System.out.println("Please enter in the value constraints (ColumnName Operator Value): ");
                    valueConstraint = "C = 6"; // scanner.nextLine();

                    System.out.println("Please enter in the number of buffers: ");
                    numBuf = "1"; //scanner.nextLine();

                    System.out.println("Please enter in the access type (\"BITMAP\", or \"CBITMAP\"): ");
                    accessType = "BITMAP"; //scanner.nextLine();

                    queryArgs = new String[]{colDBName, columnarFileName, columnName, valueConstraint, numBuf, accessType};
                    Query(queryArgs, columnarFile);
                    break;
                case "4":
                    System.out.println("Welcome to Delete Query!");

                    System.out.println("Please enter in the name of the Column DB: ");
                    colDBName = scanner.nextLine();

                    System.out.println("Please enter in the name of the Columnar File: ");
                    columnarFileName = scanner.nextLine();

                    System.out.println("Please enter in the Target Column Name (optional): ");
                    columnName = scanner.nextLine();

                    System.out.println("Please enter in the value constraints (ColumnName Operator Value): ");
                    valueConstraint = scanner.nextLine();

                    System.out.println("Please enter in the number of buffers: ");
                    numBuf = scanner.nextLine();

                    System.out.println("Please enter in the access type (\"BITMAP\", or \"CBITMAP\"): ");
                    accessType = scanner.nextLine();

                    queryArgs = new String[]{colDBName, columnarFileName, columnName, valueConstraint, numBuf, accessType};
                    deleteQuery(queryArgs, columnarFile);
                    break;
                case "5":
                    System.out.println("Quitting...");
                    break;
            }
        } while (!option.equals("5"));


        scanner.close();
    }

    public static Columnarfile batchInsert(String[] args) {
        PCounter.initialize();
        Columnarfile cf = null;

        try {

            if (args.length != 4) {
                System.out.println("Usage: java BatchInsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");
                System.exit(1);
            }
            String datafileName = args[0] + ".txt";
            String columnDBName = args[1] + "DB";
            String columnarfileName = args[2];
            int numColumns = Integer.parseInt(args[3]);

            SystemDefs sysDefs = new SystemDefs(columnDBName, 100000, 100, "Clock");

            BufferedReader br = new BufferedReader(new FileReader(datafileName));

            String firstLine = br.readLine();

            String[] columns = firstLine.split("\\s+");

            String[] columnNames = new String[columns.length];
            AttrType[] columnTypes = new AttrType[columns.length];
            short[] offSets = new short[columns.length];
            int byteLength = 0;
            for (int i = 0; i < columns.length; i++) {

                String[] columnDetails = columns[i].split(":");
                columnNames[i] = columnDetails[0];

                if (columnDetails[1].contains("int")) {
                    columnTypes[i] = new AttrType(AttrType.attrInteger);
                    byteLength += 4;
                    offSets[i] = 4;
                } else {
                    columnTypes[i] = new AttrType(AttrType.attrString);
                    byteLength += 25;
                    offSets[i] = 25;
                }
            }

            short[] tmpOffsets = new short[offSets.length];
            tmpOffsets[0] = 0;
            for(int i = 1; i < offSets.length; i++)
            {
                tmpOffsets[i] = (short) (offSets[i - 1] + tmpOffsets[i - 1]);
            }

            offSets = tmpOffsets;

            cf = new Columnarfile(columnarfileName, columnNames, columnNames.length, columnTypes);
            cf.tupleLength = byteLength;
            cf.tupleOffSets = offSets;

            //Read data from data file
            byte[] dataFileArray = new byte[byteLength + 6];
            int offset = 0;

            String currLine = br.readLine();
            while (currLine != null) {
                String[] splitLine = currLine.split("\\s+");
                for (int i = 0; i < columnTypes.length; i++) {
                    if (columnTypes[i].attrType == AttrType.attrInteger) {
                        Convert.setIntValue(Integer.parseInt(splitLine[i]), offset, dataFileArray);
                        offset += 4;
                    }
                    if (columnTypes[i].attrType == AttrType.attrString) {
                        Convert.setStrValue(splitLine[i], offset, dataFileArray);
                        offset += 25;
                    }
                }
                cf.insertTuple(dataFileArray);
                offset = 0;
                //System.out.println(currLine + ": " + Arrays.toString(dataFileArray) + " : Length: " + dataFileArray.length);
                Arrays.fill(dataFileArray, (byte)0);
                currLine = br.readLine();
            }
            br.close();



            TupleScan tscan = new TupleScan(cf);

            TID tid = new TID(cf.heapfiles[0].getRecCnt());
            tid.recordIDs = new RID[numColumns];

            Tuple tuple = new Tuple();
            tuple.setHdr((short) numColumns, cf.type, new short[]{25,25});
            //System.out.println("tuple a:" + tuple);
            for(int i = 0; i < numColumns; i++)
            {
                tid.recordIDs[i] = new RID();
            }
            System.out.println();
            int count = 0;
            while((tuple = tscan.getNext(tid)) != null)
            {
                count++;
                System.out.print("Record[" + count + "]  Inserted: ");
                tuple.print(columnTypes);
                System.out.println();
            }

            tscan.closetuplescan();


            // Print out the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
            System.out.println();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return cf;
    }

    public static void index(String[] args, Columnarfile cf) {
        PCounter.initialize();
        if (args.length != 4) { // checks length
            System.out.println("Usage: java Index COLUMNDBNAME COLUMNARFILENAME COLUMNAME INDEXTYPE");
            System.exit(1);
        }

        String columnDBName = args[0];
        String columnarfileName = args[1];
        String columnName = args[2];
        String indexType = (args[3]).toUpperCase();

        try {

            if (indexType.equals("BITMAP")) {
                createBitMap(cf, columnarfileName, columnName);
            } else if (indexType.equals("CBITMAP")) {
                createCBitMap(cf, columnarfileName, columnName);
            } else {
                System.out.println("Usage: INDEXTYPE = \"BITMAP\", or \"CBITMAP\"");
                System.exit(1);
            }

            // Print out the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createBTree(Columnarfile cf, String columnarfileName, String columnName) {
        try {
            int columnNum = -1;

            for (int i = 0; i < cf.columnNames.length; i++) {      // find column number from name
                if (cf.columnNames[i].equals(columnName)) {
                    columnNum = i;
                }
            }
            if (columnNum == -1) {
                System.out.println("Column not found for createBTree function");
                return;
            }

            if(cf.createBTreeIndex(columnNum)) // creates BTreeIndex
            {
                System.out.println("BTREE Index successful");
            }
            else {
                System.out.println("BTREE Index Failed");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createBitMap(Columnarfile cf, String columnarfileName, String columnName) {
        try {
            ColumnarFileMetadata columnarMetadata = cf.getColumnarFileMetadata();
            cf.columnNames = columnarMetadata.columnNames;

            int columnNum = -1;
            IntegerValueClass valueI = new IntegerValueClass();
            StringValueClass valueS = new StringValueClass();

            //Loops through all columns
            for (int i = 0; i < cf.columnNames.length; i++) {
                if (cf.columnNames[i].equals(columnName)) {
                    columnNum = i;
                }
            }
            if (columnNum == -1) {
                System.out.println("Column not found for createBitMap function");
                return;
            }

            //Loops through records in a heapfile
            RID rid = new RID();
            Scan s = cf.heapfiles[columnNum].openScan();
            Tuple tuple = s.getNext(rid);

            //Find range to create bitmap for each value1
            int maxValue = Integer.MIN_VALUE;
            int minValue = Integer.MAX_VALUE;
            ArrayList<Integer> intValuesList = new ArrayList<>();
            ArrayList<String> strValuesList = new ArrayList<>();

            //Add value to array
            while (tuple != null) {
                if (cf.type[columnNum].attrType == AttrType.attrInteger) {
                    int value = Convert.getIntValue(0, tuple.getTupleByteArray());
                    intValuesList.add(value);
                } else if (cf.type[columnNum].attrType == AttrType.attrString) {
                    String value = Convert.getStrValue(0, tuple.getTupleByteArray(), 25);
                    strValuesList.add(value);
                }
                tuple = s.getNext(rid);
            }

            //Int range
            Integer[] intValArray = intValuesList.toArray(new Integer[0]);
            for (int val : intValArray) {
                minValue = Math.min(minValue, val);
                maxValue = Math.max(maxValue, val);
            }

            int range = maxValue - minValue + 1;
            cf.intBitmapRange = range;


            //Str range (number of distinct strings)
            String[] strValArray = strValuesList.toArray(new String[0]);
            Set<String> set = new HashSet<>(strValuesList);
            List<String> distinctStr = new ArrayList<>(set);
            Map<String, Integer> stringID = new HashMap<>();
            for (int i = 0; i < distinctStr.size(); i++) {
                stringID.put(distinctStr.get(i), i);
            }
            cf.strBitmapRange = distinctStr.size();
            cf.stringHashMap = stringID;

            //TODO - Delete
            if(cf.type[columnNum].attrType == AttrType.attrString) {
                System.out.println("List without duplicates: " + distinctStr);
                System.out.println("String bitmap range: " + cf.strBitmapRange);
                System.out.println("Map of strings: " + stringID);
            }
            else
            {
                System.out.println("Int Range: " + range);
            }

            //Loop through each value in the array and call createBitMapIndex on it
            for (int i = 0; i < cf.heapfiles[columnNum].getRecCnt(); i++) {
                if (cf.type[columnNum].attrType == AttrType.attrInteger) {
                    valueI.setValue(intValArray[i]);
                    cf.createBitMapIndex(columnNum, valueI);
                } else if (cf.type[columnNum].attrType == AttrType.attrString) {
                    valueS.setValue(strValArray[i]);
                    cf.createBitMapIndex(columnNum, valueS);
                }
            }

            if(GlobalDebug.debug)
            {
               cf.BMFiles[columnNum].toString();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createCBitMap(Columnarfile cf, String columnarfileName, String columnName)
    {
        try {
            ColumnarFileMetadata columnarMetadata = cf.getColumnarFileMetadata();
            cf.columnNames = columnarMetadata.columnNames;

            int columnNum = -1;
            IntegerValueClass valueI = new IntegerValueClass();
            StringValueClass valueS = new StringValueClass();

            //Loops through all columns
            for (int i = 0; i < cf.columnNames.length; i++) {
                if (cf.columnNames[i].equals(columnName)) {
                    columnNum = i;
                }
            }
            if (columnNum == -1) {
                System.out.println("Column not found for createBitMap function");
                return;
            }

            //Loops through records in a heapfile
            RID rid = new RID();
            Scan s = cf.heapfiles[columnNum].openScan();
            Tuple tuple = s.getNext(rid);

            //Find range to create bitmap for each value1
            int maxValue = Integer.MIN_VALUE;
            int minValue = Integer.MAX_VALUE;
            ArrayList<Integer> intValuesList = new ArrayList<>();
            ArrayList<String> strValuesList = new ArrayList<>();

            //Add value to array
            while (tuple != null) {
                if (cf.type[columnNum].attrType == AttrType.attrInteger) {
                    int value = Convert.getIntValue(0, tuple.getTupleByteArray());
                    intValuesList.add(value);
                } else if (cf.type[columnNum].attrType == AttrType.attrString) {
                    String value = Convert.getStrValue(0, tuple.getTupleByteArray(), 25);
                    strValuesList.add(value);
                }
                tuple = s.getNext(rid);
            }

            //String
            String[] strValArray = strValuesList.toArray(new String[0]);
            Set<String> set = new HashSet<>(strValuesList);
            List<String> distinctStr = new ArrayList<>(set);
            Map<String, Integer> stringID = new HashMap<>();
            for (int i = 0; i < distinctStr.size(); i++) {
                stringID.put(distinctStr.get(i), i);
            }
            cf.strBitmapRange = distinctStr.size();
            cf.stringHashMap = stringID;

            //Int range
            Integer[] intValArray = intValuesList.toArray(new Integer[0]);
            for (int val : intValArray) {
                minValue = Math.min(minValue, val);
                maxValue = Math.max(maxValue, val);
            }
            cf.intBitmapRange = maxValue - minValue + 1;


            if(cf.type[columnNum].attrType == AttrType.attrInteger)
            {
                for(int i = 0; i < intValArray.length; i++)
                {
                    valueI.setValue(intValArray[i]);
                    cf.createCBitMapIndex(columnNum, valueI);
                }
            } else if(cf.type[columnNum].attrType == AttrType.attrString) {
                cf.strCBitmapRange = 6;
                for(int i = 0; i < strValArray.length; i++)
                {
                    valueS.setValue(strValArray[i]);
                    cf.createCBitMapIndex(columnNum, valueS);
                }
            }

            if(GlobalDebug.debug)
            {
                BitMapFile tmpBMF = new BitMapFile("sd2_" + columnNum);
                BitMapHeaderPage bmhp = tmpBMF.getHeaderPage();
                RID tmpRID = new RID();
                tmpRID = bmhp.firstRecord();
                Tuple t = new Tuple();
                do {
                    t = bmhp.getRecord(tmpRID);
                    byte[] data = t.getTupleByteArray();
                    System.out.println("Data: " + Arrays.toString(data));
                } while ((tmpRID = bmhp.nextRecord(tmpRID)) != null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void Query(String[] args, Columnarfile cf) throws UnknownKeyTypeException, InvalidTupleSizeException, InvalidTypeException {
        PCounter.initialize();

        try {
            if (args.length != 6) {
                System.out.println("Usage: java Query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE");
                System.exit(1);
            }
            String columnDBName = args[0];
            String columnarFileName = args[1];
            String targetColumns = args[2];
            String[] targetColNames = targetColumns.split(",");
            String valueConstraints = args[3];
            int numBuf = Integer.parseInt(args[4]);
            String accessType = args[5];


            //For each, access DB accordingly and check the value constraint
            switch (accessType.toUpperCase()) {
                case "FILESCAN":
                    System.out.println("ValueConstraints passing in: " + valueConstraints);
                    performFileScan(columnarFileName, targetColNames, valueConstraints, cf);
                    break;
                case "COLUMNSCAN":
                    performColumnScan(columnarFileName, targetColNames, valueConstraints, cf);
                    break;
                case "BTREE":
                    performBTreeScan(columnarFileName, targetColNames, valueConstraints, cf);
                    break;
                case "BITMAP":
                    performBitmapScan(columnarFileName, targetColNames, valueConstraints, cf);
                    break;
                case "CBITMAP":
                    performCBitmapScan(columnarFileName, targetColNames, valueConstraints, cf);
                    break;
                default:
                    System.err.println("Invalid access type");
                    break;
            }

            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());

        } catch (NumberFormatException | IOException | IndexException | UnknownIndexTypeException |
                 PageNotReadException | UnknowAttrType | FieldNumberOutOfBoundException | PredEvalException |
                 WrongPermat | InvalidRelation | FileScanException | TupleUtilsException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteQuery(String[] args, Columnarfile cf) throws UnknownKeyTypeException, InvalidTupleSizeException, InvalidTypeException {
        PCounter.initialize();

        try {
            if (args.length != 6) {
                System.out.println("Usage: java deleteQuery COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE");
                System.exit(1);
            }
            String columnDBName = args[0];
            String columnarFileName = args[1];
            String targetColumns = args[2];
            String[] targetColNames = targetColumns.split(",");
            String valueConstraints = args[3];
            int numBuf = Integer.parseInt(args[4]);
            String accessType = args[5];

            switch (accessType.toUpperCase()) {
                case "FILESCAN":
                    System.out.println("ValueConstraints passing in: " + valueConstraints);
                    performFileScan(columnarFileName, targetColNames, valueConstraints, cf);
                    break;
                case "COLUMNSCAN":
                    performColumnScan(columnarFileName, targetColNames, valueConstraints, cf);
                    break;
                case "BTREE":
                    performBTreeScan(columnarFileName, targetColNames, valueConstraints, cf);
                    break;
                case "BITMAP":
                    performBitmapScan(columnarFileName, targetColNames, valueConstraints, cf);
                    break;
                case "CBITMAP":
                    performCBitmapScan(columnarFileName, targetColNames, valueConstraints, cf);
                    break;
                default:
                    System.err.println("Invalid access type");
                    break;
            }

            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());

        } catch (NumberFormatException | IOException | IndexException | UnknownIndexTypeException |
                 PageNotReadException | UnknowAttrType | FieldNumberOutOfBoundException | PredEvalException |
                 WrongPermat | InvalidRelation | FileScanException | TupleUtilsException e) {
            throw new RuntimeException(e);
        }
    }


    private static void performFileScan(String columnarFileName, String[] targetColumnNames, String valueConstraint, Columnarfile cf) {
        try {

            String[] columns = cf.columnNames;

            //String[] columnNames = new String[targetColumnNames.length];
            AttrType[] attrTypes = cf.type;
            AttrType[] type = new AttrType[1];

            FldSpec[] projList = new FldSpec[1];
            projList[0] = new FldSpec(new RelSpec(RelSpec.outer),  1);

            //obtains value constraints and ops
            CondExpr[] valueConstraintExpr = new CondExpr[1];
            valueConstraintExpr[0] = new CondExpr();
            String[] values = valueConstraint.split(" ");
            System.out.println("ValueConstraints passing in: " + Arrays.toString(values));
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
                type[0] = attrTypes[0];
            }
            else if(Objects.equals(columnName, "B"))
            {
                columnNum = 1;
                type[0] = attrTypes[1];
            }
            else if(Objects.equals(columnName, "C")) {
                columnNum = 2;
                type[0] = attrTypes[2];
            }
            else if(Objects.equals(columnName, "D"))
            {
                columnNum = 3;
                type[0] = attrTypes[3];
            }

            short[] sSizes = new short[]{50};
            if(type[0].attrType == AttrType.attrInteger)
            {
                sSizes[0] = 4;
            }
            else {
                sSizes[0] = 25;
            }

            valueConstraintExpr[0].type1 = new AttrType(AttrType.attrString);
            valueConstraintExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), columnNum);
            valueConstraintExpr[0].operand1.string = columnName;

            if(value.matches("^\\d+$"))
            {
                valueConstraintExpr[0].type2 = new AttrType(AttrType.attrInteger);
                valueConstraintExpr[0].operand2.integer = Integer.parseInt(value);
            }
            else
            {
                valueConstraintExpr[0].type2 = new AttrType(AttrType.attrString);
                valueConstraintExpr[0].operand2.string = value;
            }


            String fileName = columnarFileName + ".columnid" + columnNum;

            FileScan fileScan = new FileScan(
                    fileName,
                    type,
                    sSizes,
                    (short) 1,
                     1,
                    projList,
                    valueConstraintExpr
            );

            System.out.println(fileScan.toString());

            // Get tuples one by one
            Tuple tuple = new Tuple();

            tuple.setTuple_length(cf.tupleLength);
            tuple.setFldCnt((short)cf.type.length);
            tuple.setFldsOffset(cf.tupleOffSets);

            while ((tuple = fileScan.get_next()) != null) {
                // Process the tuple here
                tuple.print(type);
            }

            // Close the file scan
            fileScan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }



    }

    private static void performColumnScan(String columnarFileName, String[] targetColumnNames, String valueConstraint, Columnarfile cf) throws InvalidTupleSizeException, IOException, InvalidTypeException, UnknownKeyTypeException, IndexException, UnknownIndexTypeException, PageNotReadException, UnknowAttrType, FieldNumberOutOfBoundException, PredEvalException, WrongPermat, InvalidRelation, FileScanException, TupleUtilsException {
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
            String[] columns = cf.columnNames;

            AttrType[] columnTypes = cf.type;


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

            //String columnarFileName = this.name + ".columnid" + i;
            ColumnarFileScan columnarFileScan = new ColumnarFileScan(
                    columnarFileName,
                    attrTypes,
                    strSizes,
                    (short) noInFlds,
                    noOutFlds,
                    outFlds,
                    valueConstraintExpr
            );

            // Perform column scan
            Tuple tuple;
            while ((tuple = columnarFileScan.get_next()) != null) {
                System.out.println(tuple);
            }

            columnarFileScan.close();
        }
    }

    private static void performBTreeScan(String columnarFileName, String[] targetColumnNames, String valueConstraint, Columnarfile cf) {
        try {
            BTreeFile btreeFile = cf.bTreeFiles[0];
            BTFileScan btreeScan = btreeFile.new_scan(null, null);

            // Iterate over the B-tree index entries
            KeyDataEntry entry;
            while ((entry = btreeScan.get_next()) != null) {
                if(entry.key instanceof IntegerKey) {
                    IntegerKey key = (IntegerKey) entry.key;
                    RID rid = ((LeafData) entry.data).getData();
                    System.out.println("Key: " + key.getKey() + ", RID: " + rid.toString());
                }
                else {
                    StringKey key = (StringKey) entry.key;
                    RID rid = ((LeafData) entry.data).getData();
                    System.out.println("Key: " + key.getKey() + ", RID: " + rid.toString());
                }

            }

            // Close the B-tree scan
            btreeScan.DestroyBTreeFileScan();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void performBitmapScan(String columnarFileName, String[] targetColumnNames, String valueConstraint, Columnarfile cf) {
        try {
            //obtains value constraints and ops
            CondExpr[] valueConstraintExpr = new CondExpr[1];
            valueConstraintExpr[0] = new CondExpr();
            String[] values = valueConstraint.split(" ");
            //TODO check value constraint
            System.out.println("ValueConstraints passing in: " + Arrays.toString(values));
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

            valueConstraintExpr[0].type1 = new AttrType(AttrType.attrString);
            valueConstraintExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), columnNum);
            valueConstraintExpr[0].operand1.string = columnName;

            if(value.matches("^\\d+$"))
            {
                valueConstraintExpr[0].type2 = new AttrType(AttrType.attrInteger);
                valueConstraintExpr[0].operand2.integer = Integer.parseInt(value);
            }
            else
            {
                valueConstraintExpr[0].type2 = new AttrType(AttrType.attrString);
                valueConstraintExpr[0].operand2.string = value;
            }

            //TODO
            if(cf.type[columnNum].attrType == AttrType.attrString) {
                System.out.println(valueConstraintExpr[0].operand1.string + ", " + valueConstraintExpr[0].operand2.string);
            }
            else{
                System.out.println(valueConstraintExpr[0].operand1.string + ", " + valueConstraintExpr[0].operand2.integer);
            }


            BitMapFile tmpBMF = new BitMapFile("sd2_" + columnNum);
            BitMapHeaderPage bmhp = tmpBMF.getHeaderPage();
            RID tmpRID = new RID();
            tmpRID = bmhp.firstRecord();
            Tuple t = new Tuple();

            int[] tupleMatchPos = new int[cf.heapfiles[columnNum].getRecCnt()];
            for(int i = 0; i < tupleMatchPos.length; i++)
            {
                tupleMatchPos[i] = -1;
            }
            int recIndex = 0;
            int tmpPos = 0;
            do {
                t = bmhp.getRecord(tmpRID);
                byte[] data = t.getTupleByteArray();
                if(cf.type[columnNum].attrType == AttrType.attrString)
                {
                    int posValue = cf.stringHashMap.get(value);
                    if (data.length == cf.stringHashMap.size() && data[posValue] == 1) {
                        tupleMatchPos[tmpPos] = recIndex;
                        tmpPos++;
                    }
                }
                else {
                    if (data.length == cf.intBitmapRange && data[Integer.parseInt(value)] == 1) {
                        tupleMatchPos[tmpPos] = recIndex;
                        tmpPos++;
                    }
                }
                recIndex++;

            } while ((tmpRID = bmhp.nextRecord(tmpRID)) != null);
            for(int i = 0; i < tupleMatchPos.length; i++)
            {
                if(tupleMatchPos[i] != -1)
                {
                    System.out.println("Match found; index: " + tupleMatchPos[i] + " for value: " + value);
                }
            }



            tmpBMF.close(); // Close the bitmap file after scanning
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void performCBitmapScan(String columnarFileName, String[] targetColumnNames, String valueConstraint, Columnarfile cf) {

    }
}

