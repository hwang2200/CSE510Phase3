package programs;

import btree.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import value.IntegerValueClass;
import value.StringValueClass;
import value.ValueClass;
import index.*;
import columnar.*;
import java.io.*;
import java.util.Objects;
import java.util.Scanner;

import java.io.*;

public class index {

    public static void main(String[] args) {
        PCounter.initialize();
        Scanner scanner = new Scanner(System.in);

        System.out.println("Welcome to programs.index!");
        System.out.println("Please enter in a query in the format: COLUMNDBNAME COLUMNARFILENAME COLUMNAME INDEXTYPE");

        System.out.println("Please enter in the Name of the Column DB: ");
        String colDBName = scanner.nextLine();

        System.out.println("Please enter in the Column File Name: ");
        String colFileName = scanner.nextLine();

        System.out.println("Please enter in the Target Column Name: ");
        String columnName = scanner.nextLine();

        System.out.println("Please enter in the Index Type (\"BTREE\", or \"BITMAP\": ");
        String indexType = scanner.nextLine();

        String[] queryArgs = {colDBName, colFileName, columnName, indexType};
        indexStart(queryArgs);

        scanner.close();
    }
    public static void indexStart(String[] args){
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
            // TODO: handle exception
        }
    }


    public static void createBTree(String columnarfileName, String columnName){
        try {
            BufferedReader br = new BufferedReader(new FileReader(columnarfileName));
            String[] columns = br.readLine().split(" ");    // read in column 
            int columnNum = 0;

            for (int i = 0; i < columns.length; i++) {      // find column number from name
                String[] columnNames = columns[i].split(":");

                if (columnNames[0].equals(columnName)) {
                    columnNum = i;
                }
            }

            Columnarfile cf = new Columnarfile(columnarfileName, columnNum, null);    // create cf with name and number
            cf.createBTreeIndex(columnNum); // creates BTreeIndex
            br.close(); // closes br

        } catch (Exception e) {
            e.printStackTrace();
            // TODO: handle exception
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
            // TODO: handle exception
        }
    }
}