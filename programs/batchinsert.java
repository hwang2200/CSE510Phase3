package programs;

import columnar.Columnarfile;
import diskmgr.ColumnDB;
import global.AttrType;
import global.Convert;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.*;

public class batchinsert
{
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Usage: java BatchInsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");
            System.exit(1);
        }

        String datafileName = args[0];
        String columnDBName = args[1];
        String columnarfileName = args[2];
        int numColumns = Integer.parseInt(args[3]);

        if (datafileName != null || columnarfileName != null) {
            ColumnDB cDB = new ColumnDB();
            cDB.openColumnDB(columnDBName);
            
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
                    columnTypes[i] = new AttrType(AttrType.attrInteger);
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
        }
    }
}
