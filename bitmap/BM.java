package bitmap;

import java.io.*;
import btree.*;
import global.*;
import diskmgr.*;
import bufmgr.*;
import heap.*;

public class BM implements GlobalConst {
	public static void printBitMap(bitmap.BitMapHeaderPage header) 
	    throws IOException, 
	   	ConstructPageException, 
	   	IteratorException,
	   	HashEntryNotFoundException,
	   	InvalidFrameNumberException,
	   	PageUnpinnedException,
	   	ReplacerException 
	{
		if (header.get_rootId().pid == INVALID_PAGE) {
			System.out.println("The Bitmap is Empty!!!");
			return;
		}

		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("---------------The Bitmap---------------");

		

		System.out.println("------------------ End -----------------");
	}
}
