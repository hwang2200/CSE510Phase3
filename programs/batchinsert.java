package programs;

import columnar.Columnarfile;
import global.AttrType;

import java.io.*;

public class batchinsert
{
    public static void main(String[] args) throws IOException {
        if (args.length != 4)
        {
            System.out.println("Usage: java BatchInsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");
            System.exit(1);
        }

        String datafileName = args[0];
        String columnDBName = args[1];
        String columnarfileName = args[2];
        int numColumns = Integer.parseInt(args[3]);

        if(datafileName != null || columnarfileName != null)
        {
            BufferedReader br = new BufferedReader(new FileReader(datafileName));
            String[] columns = br.readLine().split(" ");

            String[] columnNames = new String[numColumns];
            AttrType[] columnTypes = new AttrType[numColumns];

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

            Columnarfile cf = new Columnarfile(columnarfileName, numColumns, );
        }
}
