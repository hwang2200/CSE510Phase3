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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Scanner;

public class batchinsert
{
    public static void main(String[] args) {
        PCounter.initialize();
        Scanner scanner = new Scanner(System.in);


        System.out.println("Welcome to batchinsert!");
        //System.out.println("Please enter in a query in the format: DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");

        System.out.println("Please enter in the Name of the Data File: ");
        String dataFileName = scanner.nextLine();

        //System.out.println("Please enter in the Name of the Column DB: ");
        String colDBName = "DB" + dataFileName;// scanner.nextLine();

        String[] queryArgs = {dataFileName, colDBName};
        batchInsert(queryArgs);

        scanner.close();
    }

    public static Columnarfile batchInsert(String[] args){
        PCounter.initialize();
        Columnarfile cf = null;

        try {

            if (args.length != 2) {
                System.out.println("Usage: java BatchInsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");
                System.exit(1);
            }
            String datafileName = args[0];
            String columnDBName = args[1];
            String columnarfileName = "Columnar" + datafileName;

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
}
