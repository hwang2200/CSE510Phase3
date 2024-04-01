package programs;

import columnar.Columnarfile;
import diskmgr.ColumnDB;
import diskmgr.PCounter;
import global.AttrType;
import global.Convert;
import global.SystemDefs;
import value.IntegerValueClass;
import value.StringValueClass;
import value.ValueClass;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Scanner;

public class allPrograms {
    public static void main(String[] args)
    {
        Columnarfile columnarFile = null;
        Scanner scanner = new Scanner(System.in);

        System.out.println("1. Batch Insert Program");
        System.out.println("2. Index Program");
        System.out.println("3. Query Program");
        System.out.println("4. Delete Query Program");
        System.out.println("5. Quit Program");
        System.out.println();
        System.out.print("Enter Option from above: ");
        int option = scanner.nextInt();

        do {
            if (option == 1) {
                Scanner scan1 = new Scanner(System.in);
                System.out.println("Welcome to batchinsert.index!");
                //System.out.println("Please enter in a query in the format: DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");

                System.out.println("Please enter in the name of the Data File: ");
                String dataFileName = scan1.nextLine();

                System.out.println("Please enter in the name of the Column DB: ");
                String colDBName = scan1.nextLine();

                System.out.println("Please enter in the name of the Columnar File: ");
                String columnarFileName = scan1.nextLine();

                System.out.println("Please enter in the number of columns: ");
                String numColumns = scan1.nextLine();

                String[] queryArgs = {dataFileName, colDBName, columnarFileName, numColumns};

                columnarFile = batchInsert(queryArgs);
                //System.out.println(columnarFile);

                scan1.close();
                break;
            } else if (option == 2) {
                Scanner scan2 = new Scanner(System.in);

                //column DB name = specified above
                //columnarfile name = returned by batchinsert

                System.out.println("Welcome to index!");
                //System.out.println("Please enter in a query in the format: COLUMNDBNAME COLUMNARFILENAME COLUMNAME INDEXTYPE");

                System.out.println("Please enter in the name of the Column DB: ");
                String colDBName = scan2.nextLine();

                //System.out.println("Please enter in the name of the Columnar File: ");
                //String columnarFileName = scan2.nextLine();

                System.out.println("Please enter in the Target Column Name: ");
                String columnName = scan2.nextLine();

                System.out.println("Please enter in the Index Type (\"BTREE\", or \"BITMAP\"): ");
                String indexType = scan2.nextLine();

                String[] queryArgs = {colDBName, columnarFile, columnName, indexType};
                index(queryArgs);

                scan2.close();
                break;
            } else if (option == 3) {

            } else if (option == 4) {

            } else if (option == 5) {
                System.out.println("Quitting...");
                break;
            }
        } while (option != 5);


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
            String datafileName = args[0];
            String columnDBName = args[1];
            String columnarfileName = args[2];
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

            cf = new Columnarfile(columnarfileName, columnNames.length, columnTypes);
            
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

    public static void index(String[] args){
        PCounter.initialize();
        Scanner scanner = new Scanner(System.in);
        if (args.length != 4) { // checks length
            System.out.println("Usage: java Index COLUMNDBNAME COLUMNARFILENAME COLUMNAME INDEXTYPE");
            System.exit(1);
        }

        String columnDBName = args[0];
        String columnarfileName = args[1];
        String columnName = args[2];
        String indexType = (args[3]).toUpperCase();

        try {
            SystemDefs sysDefs = new SystemDefs(columnDBName, 100000, 100, "Clock");
            ColumnDB testDB = new ColumnDB();   // open columnDB and type of index
            testDB.openDB(columnDBName);

            if(indexType.equals("BTREE")){
                createBTree(columnarfileName, columnName);
            }
            else if(indexType.equals("BITMAP")){
                createBitMap(columnarfileName, columnName);
            }
            else{
                System.out.println("Usage: INDEXTYPE = BTREE or BITMAP");
                System.exit(1);
            }

            // Print out the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
            scanner.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createBTree(String columnarfileName, String columnName){
        try {
            BufferedReader br = new BufferedReader(new FileReader(columnarfileName));
            String[] columns = br.readLine().split(" ");    // read in column
            System.out.println(Arrays.toString(columns));

            int columnNum = 0;

            for (int i = 0; i < columns.length; i++) {      // find column number from name
                String[] columnNames = columns[i].split(":");
                System.out.println(Arrays.toString(columnNames));

                if (columnNames[0].equals(columnName)) {
                    columnNum = i;
                }
            }

            Columnarfile cf = new Columnarfile(columnarfileName, columnNum, null);    // create cf with name and number
            cf.createBTreeIndex(columnNum); // creates BTreeIndex
            br.close(); // closes br

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createBitMap(String columnarfileName, String columnName){
        try {
            BufferedReader br = new BufferedReader(new FileReader(columnarfileName));
            String[] columns = br.readLine().split(" ");    // read in columns
            int columnNum = 0;
            boolean intOrString = false;
            ValueClass valueI = new IntegerValueClass();
            ValueClass valueS = new StringValueClass();


            for (int i = 0; i < columns.length; i++) {
                String[] columnNames = columns[i].split(":");   // find column name

                if (columnNames[0].equals(columnName)) {
                    columnNum = i;
                    if (columnNames[1].equals("int")){  // need type either int or string
                        intOrString = false;
                    }
                    else{
                        intOrString = true;
                    }
                }
            }

            Columnarfile cf = new Columnarfile(columnarfileName, columnNum, null);
            if(intOrString){
                cf.createBitMapIndex(columnNum, valueI);    // bitmap index with int
            }
            else{
                cf.createBitMapIndex(columnNum, valueS);        // bitmap index with string
            }

            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
