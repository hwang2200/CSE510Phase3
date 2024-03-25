package programs;

import columnar.Columnarfile;
import diskmgr.*;
import global.AttrType;
import global.Convert;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.*;
import java.util.Scanner;

public class batchinsert
{
    public static void main(String[] args) {
        PCounter.initialize();
        Scanner scanner = new Scanner(System.in);

        System.out.println("Welcome to batchinsert.index!");
        System.out.println("Please enter in a query in the format: DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");

        System.out.println("Please enter in the Name of the Data File: ");
        String dataFileName = scanner.nextLine();

        System.out.println("Please enter in the Name of the Column DB: ");
        String colDBName = scanner.nextLine();

        System.out.println("Please enter in the Column File Name: ");
        String colFileName = scanner.nextLine();

        System.out.println("Please enter in the number of columns: ");
        String readInColumns = scanner.nextLine();        

        String[] queryArgs = {dataFileName, colDBName, colFileName, readInColumns};
        batchInsert(queryArgs);

        scanner.close();
    }

    public static void batchInsert(String[] args){
        PCounter.initialize();
        try {

            if (args.length != 4) {
                System.out.println("Usage: java BatchInsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");
                System.exit(1);
            }
            String datafileName = args[0];
            String columnDBName = args[1];
            String columnarfileName = args[2];
            int numColumns = Integer.parseInt(args[3]);

            //Initialize by making init() static in SystemDefs.java (but what are the proper parameters for batch insert?)
            SystemDefs.init(columnDBName, "test", 0, 0, 0, "Clock");
            ColumnDB cDB = new ColumnDB();
            cDB.openDB(columnDBName);

                
            BufferedReader br = new BufferedReader(new FileReader(datafileName));
            String[] columns = br.readLine().split(" ");

            String[] columnNames = new String[numColumns];
            AttrType[] columnTypes = new AttrType[numColumns];
            

            for (int i = 0; i < columns.length; i++) {
                String[] columnDetails = columns[i].split(":");
                columnNames[i] = columnDetails[0];

                if (columnDetails[1].equals("int")) {
                    columnTypes[i] = new AttrType(AttrType.attrInteger);
                } else {
                    columnTypes[i] = new AttrType(AttrType.attrString);
                }
            }

            Columnarfile cf = new Columnarfile(columnarfileName, numColumns, columnTypes);

            //Read data from data file
            byte[] dataFileArray = new byte[numColumns];
            int offset = 0;
            
            while (br.readLine() != null) {
                    for (int i = 0; i < columnTypes.length; i++) {
                        if (columnTypes[i].attrType == AttrType.attrInteger) {
                            Convert.setIntValue(Integer.parseInt(columns[i + 1]), offset, dataFileArray);
                            offset += 4;
                        }
                        if (columnTypes[i].attrType == AttrType.attrString) {
                            Convert.setStrValue(columns[i + 1], offset, dataFileArray);
                            offset += columns[i + 1].length();
                        }
                    }
                    cf.insertTuple(dataFileArray);
                }
            br.close();

            // Print out the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }
}
