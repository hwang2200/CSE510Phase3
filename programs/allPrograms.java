package programs;

import btree.BTreeFile;
import columnar.ColumnarFileMetadata;
import columnar.Columnarfile;
import diskmgr.ColumnDB;
import diskmgr.PCounter;
import global.AttrType;
import global.Convert;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import org.w3c.dom.Attr;
import value.IntegerValueClass;
import value.StringValueClass;
import value.ValueClass;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class allPrograms {
    public static void main(String[] args)
    {
        Columnarfile columnarFile = null;
        Scanner scanner = new Scanner(System.in);
        String option = null;

        do {

            System.out.println("1. Batch Insert Program");
            System.out.println("2. Index Program");
            System.out.println("3. Query Program");
            System.out.println("4. Delete Query Program");
            System.out.println("5. Quit Program");
            System.out.println();
            System.out.print("Enter Option from above: ");
            option = scanner.nextLine();

            switch(option)
            {
                case "1":
                    //Scanner scan1 = new Scanner(System.in);
                    System.out.println("Welcome to batchinsert!");
                    //System.out.println("Please enter in a query in the format: DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");

                    System.out.println("Please enter in the name of the Data File: ");
                    String dataFileName = scanner.nextLine();

                    System.out.println("Please enter in the name of the Column DB: ");
                    String colDBName = scanner.nextLine();

                    System.out.println("Please enter in the name of the Columnar File: ");
                    String columnarFileName = scanner.nextLine();

                    System.out.println("Please enter in the number of columns: ");
                    String numColumns = scanner.nextLine();

                    String[] queryArgs = {dataFileName, colDBName, columnarFileName, numColumns};

                    columnarFile = batchInsert(queryArgs);
                    //System.out.println(columnarFile.toString());

                    //scan1.close();
                    break;
                case "2":
                    //Scanner scan2 = new Scanner(System.in);

                    //column DB name = specified above
                    //columnarfile name = returned by batchinsert

                    System.out.println("Welcome to index!");
                    //System.out.println("Please enter in a query in the format: COLUMNDBNAME COLUMNARFILENAME COLUMNAME INDEXTYPE");

                    System.out.println("Please enter in the name of the Column DB: ");
                    colDBName = scanner.nextLine();

                    System.out.println("Please enter in the name of the Columnar File: ");
                    columnarFileName = scanner.nextLine();

                    System.out.println("Please enter in the Target Column Name: ");
                    String columnName = scanner.nextLine();

                    System.out.println("Please enter in the Index Type (\"BTREE\", or \"BITMAP\"): ");
                    String indexType = scanner.nextLine();

                    queryArgs = new String[]{colDBName, columnarFileName, columnName, indexType};
                    index(queryArgs, columnarFile);

                    //scan2.close();
                    break;
                case "3":

                case "4":

                case "5":
                    System.out.println("Quitting...");
                    break;
            }
        } while (!option.equals("5"));


        scanner.close();
    }

    public static Columnarfile batchInsert(String[] args){
        PCounter.initialize();
        Columnarfile cf = null;

        try {

            if (args.length != 4) {
                System.out.println("Usage: java BatchInsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");
                System.exit(1);
            }
            String datafileName = args[0] + ".txt";
            String columnDBName = args[1] + "DB";
            String columnarfileName = args[2] + "COL";
            int numColumns = Integer.parseInt(args[3]);

            SystemDefs sysDefs = new SystemDefs(columnDBName, 100000, 100, "Clock");

            BufferedReader br = new BufferedReader(new FileReader(datafileName));

            String firstLine = br.readLine();

            String[] columns = firstLine.split("\\s+");

            String[] columnNames = new String[columns.length];
            AttrType[] columnTypes = new AttrType[columns.length];

            for (int i = 0; i < columns.length; i++) {

                String[] columnDetails = columns[i].split(":");
                columnNames[i] = columnDetails[0];

                if (columnDetails[1].contains("int")) {
                    columnTypes[i] = new AttrType(AttrType.attrInteger);
                } else {
                    columnTypes[i] = new AttrType(AttrType.attrString);
                }
            }

            cf = new Columnarfile(columnarfileName, columnNames, columnNames.length, columnTypes);

            //Read data from data file
            byte[] dataFileArray = new byte[25+25+4+4];
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
                currLine = br.readLine();
            }
            br.close();

            // Print out the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return cf;
    }

    public static void index(String[] args, Columnarfile cf){
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

            if(indexType.equals("BTREE")){
                createBTree(cf, columnarfileName, columnName);
            }
            else if(indexType.equals("BITMAP")){
                createBitMap(cf, columnarfileName, columnName);
            }
            else{
                System.out.println("Usage: INDEXTYPE = BTREE or BITMAP");
                System.exit(1);
            }

            // Print out the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createBTree(Columnarfile cf, String columnarfileName, String columnName){
        try {
            int columnNum = -1;

            for (int i = 0; i < cf.columnNames.length; i++) {      // find column number from name
                if (cf.columnNames[i].equals(columnName)) {
                    columnNum = i;
                }
            }
            if(columnNum == -1)
            {
                System.out.println("Column not found for createBTree function");
                return;
            }

            cf.createBTreeIndex(columnNum); // creates BTreeIndex

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createBitMap(Columnarfile cf, String columnarfileName, String columnName){
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
            if(columnNum == -1)
            {
                System.out.println("Column not found for createBitMap function");
                return;
            }

            //Loops through records in a heapfile
            RID rid = new RID();
            Scan s = cf.heapfiles[columnNum].openScan();
            Tuple tuple = s.getNext(rid);

            //Find range to create bitmap for each value
            int maxValue = Integer.MIN_VALUE;
            int minValue = Integer.MAX_VALUE;
            ArrayList<Integer> valuesList = new ArrayList<>();

            //Add value to array
            while(tuple != null) {
                if (cf.type[columnNum].attrType == AttrType.attrInteger)
                {
                    int value = Convert.getIntValue(0, tuple.getTupleByteArray());
                    valuesList.add(value);
                }
                else if (cf.type[columnNum].attrType == AttrType.attrString)
                {

                }
                tuple = s.getNext(rid);
            }

            Integer[] valuesArray = valuesList.toArray(new Integer[0]);
            for (int val : valuesArray) {
                minValue = Math.min(minValue, val);
                maxValue = Math.max(maxValue, val);
            }

            int range = maxValue - minValue + 1;
            cf.bitmapRange = range;
            System.out.println("Range: " + range);

            //Loop through each value in the array and call createBitMapIndex on it
            for(int i = 0; i < valuesArray.length; i++)
            {
                if (cf.type[columnNum].attrType == AttrType.attrInteger)
                {
                    valueI.setValue(valuesArray[i]);
                    cf.createBitMapIndex(columnNum, valueI);
                }
                else if (cf.type[columnNum].attrType == AttrType.attrString)
                {

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
