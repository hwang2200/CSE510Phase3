package Project2;

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

    public boolean equals(TID tid1, TID tid2)
    {
        boolean result = false;
        if(tid1 == tid2)
        {
            result = true;
        }
        return result;
    }

    public void writeToByteArray(byte[] array, int offset)
    {

    }

    public void setPosition(int position)
    {

    }

    public void setRID(int column, RID recordID)
    {

    }
}
