package Project2;

import global.Convert;
import global.RID;

public class TID extends Object
{
    //Fields
    public int numRIDs;
    public int position;
    public RID[] recordIDs;

    //Constructors
    public TID(int numRIDs)
    {
        this.numRIDs = numRIDs;
    }

    public TID(int numRIDs, int position)
    {
        this.numRIDs = numRIDs;
        this.position = position;
    }

    public TID(int numRIDs, int position, RID[] recordIDs)
    {
        this.numRIDs = numRIDs;
        this.position = position;
        this.recordIDs = recordIDs;
    }

    //Methods
    public void copyTID(TID tid)
    {
        this.numRIDs = tid.numRIDs;
        this.position = tid.position;
        this.recordIDs = tid.recordIDs;
    }

    public boolean equals(TID tid)
    {
        if ((this.numRIDs == tid.numRIDs) && (this.position == tid.position) && (this.recordIDs == tid.recordIDs))
        {
            return true;
        } else {
            return false;
        }
    }

    public void writeToByteArray(byte[] array, int offset) throws java.io.IOException
    {
        Convert.setIntValue ( numRIDs, offset, array);
        Convert.setIntValue ( position, offset+4, array);
        int lastPos = offset+4;
        for(int i = 0; i < recordIDs.length; i++)
        {
            lastPos += 4;
            Convert.setIntValue ( recordIDs[i].slotNo, lastPos, array);
            lastPos += 4;
            Convert.setIntValue ( recordIDs[i].pageNo.pid, lastPos, array);
        }
    }

    public void setPosition(int position)
    {
        this.position = position;
    }

    public void setRID(int column, RID recordID)
    {
        recordIDs[column] = recordID;
    }
}
