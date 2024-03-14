package bitmap;

import java.io.*;
import btree.*;
import global.*;
import bufmgr.*;

public class BM implements GlobalConst {

	public static void printBitMap(bitmap.BitMapHeaderPage header)
			throws IOException,
			ConstructPageException,
			IteratorException,
			HashEntryNotFoundException,
			InvalidFrameNumberException,
			PageUnpinnedException,
			ReplacerException {
		if (header.getNextPage().pid == INVALID_PAGE) {
			System.out.println("The Bitmap is Empty!!!");
			return;
		}

		BMPage page = new BMPage();
		PageId nextpageno = header.getNextPage();

		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("---------------The Bitmap---------------");
		
		while (nextpageno.pid != INVALID_PAGE) {
				page.setCurPage(nextpageno);
				page.dumpPage();
				nextpageno = page.getNextPage();
			}

		System.out.println("------------------ End -----------------");
	}
}
