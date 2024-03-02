package diskmgr;

import global.GlobalConst;
import global.PageId;
import global.SystemDefs;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ColumnDB implements GlobalConst {

    public static pcounter pcounterInstance = new pcounter();
    private static final int bits_per_page = 8192;
    private RandomAccessFile fp;
    private int num_pages;
    private String name;

    public void openColumnDB(String var1) throws IOException, InvalidPageNumberException, FileIOException, DiskMgrException {
        pcounter var10000 = pcounterInstance;
        pcounter.initialize();
        this.name = var1;
        this.fp = new RandomAccessFile(var1, "rw");
        PageId var2 = new PageId();
        Page var3 = new Page();
        var2.pid = 0;
        this.num_pages = 1;
        this.pinPage(var2, var3, false);
        DBFirstPage var4 = new DBFirstPage();
        var4.openPage(var3);
        this.num_pages = var4.getNumDBPages();
        this.unpinPage(var2, false);
    }

    public ColumnDB() {}

    public void openColumnDB(String var1, int var2) throws IOException, InvalidPageNumberException, FileIOException, DiskMgrException {
        this.name = new String(var1);
        this.num_pages = var2 > 2 ? var2 : 2;
        File var3 = new File(this.name);
        var3.delete();
        this.fp = new RandomAccessFile(var1, "rw");
        this.fp.seek((long)(this.num_pages * 1024 - 1));
        this.fp.writeByte(0);
        Page var4 = new Page();
        PageId var5 = new PageId();
        var5.pid = 0;
        this.pinPage(var5, var4, true);
        DBFirstPage var6 = new DBFirstPage(var4);
        var6.setNumDBPages(this.num_pages);
        this.unpinPage(var5, true);
        int var7 = (this.num_pages + 8192 - 1) / 8192;
        this.set_bits(var5, 1 + var7, 1);
    }

    public void closeDB() throws IOException {
        this.fp.close();
    }

    public void DBDestroy() throws IOException {
        this.fp.close();
        File var1 = new File(this.name);
        var1.delete();
    }

    public void read_page(PageId var1, Page var2) throws InvalidPageNumberException, FileIOException, IOException {
        if (var1.pid >= 0 && var1.pid < this.num_pages) {
            this.fp.seek((long)(var1.pid * 1024));
            byte[] var3 = var2.getpage();

            try {
                this.fp.read(var3);
            } catch (IOException var5) {
                throw new FileIOException(var5, "DB file I/O error");
            }

            pcounter.readIncrement();
        } else {
            throw new InvalidPageNumberException((Exception)null, "BAD_PAGE_NUMBER");
        }
    }

    public void write_page(PageId var1, Page var2) throws InvalidPageNumberException, FileIOException, IOException {
        if (var1.pid >= 0 && var1.pid < this.num_pages) {
            this.fp.seek((long)(var1.pid * 1024));

            try {
                this.fp.write(var2.getpage());
            } catch (IOException var4) {
                throw new FileIOException(var4, "DB file I/O error");
            }

            pcounter.writeIncrement();
        } else {
            throw new InvalidPageNumberException((Exception)null, "INVALID_PAGE_NUMBER");
        }
    }

    public void allocate_page(PageId var1) throws OutOfSpaceException, InvalidRunSizeException, InvalidPageNumberException, FileIOException, DiskMgrException, IOException {
        this.allocate_page(var1, 1);
    }

    public void allocate_page(PageId var1, int var2) throws OutOfSpaceException, InvalidRunSizeException, InvalidPageNumberException, FileIOException, DiskMgrException, IOException {
        if (var2 < 0) {
            throw new InvalidRunSizeException((Exception)null, "Negative run_size");
        } else {
            int var3 = var2;
            int var4 = (this.num_pages + 8192 - 1) / 8192;
            int var5 = 0;
            int var6 = 0;
            PageId var7 = new PageId();

            for(int var10 = 0; var10 < var4; ++var10) {
                var7.pid = 1 + var10;
                Page var11 = new Page();
                this.pinPage(var7, var11, false);
                byte[] var8 = var11.getpage();
                int var9 = 0;
                int var12 = this.num_pages - var10 * 8192;
                if (var12 > 8192) {
                    var12 = 8192;
                }

                while(var12 > 0 && var6 < var3) {
                    Integer var13 = new Integer(1);
                    Byte var14 = new Byte(var13.byteValue());

                    for(byte var15 = var14; var14.intValue() != 0 && var12 > 0 && var6 < var3; --var12) {
                        if ((var8[var9] & var15) != 0) {
                            var5 += var6 + 1;
                            var6 = 0;
                        } else {
                            ++var6;
                        }

                        var15 = (byte)(var15 << 1);
                        var14 = new Byte(var15);
                    }

                    ++var9;
                }

                this.unpinPage(var7, false);
            }

            if (var6 >= var3) {
                var1.pid = var5;
                this.set_bits(var1, var3, 1);
            } else {
                throw new OutOfSpaceException((Exception)null, "No space left");
            }
        }
    }

    public void deallocate_page(PageId var1, int var2) throws InvalidRunSizeException, InvalidPageNumberException, IOException, FileIOException, DiskMgrException {
        if (var2 < 0) {
            throw new InvalidRunSizeException((Exception)null, "Negative run_size");
        } else {
            this.set_bits(var1, var2, 0);
        }
    }

    public void deallocate_page(PageId var1) throws InvalidRunSizeException, InvalidPageNumberException, IOException, FileIOException, DiskMgrException {
        this.set_bits(var1, 1, 0);
    }

    public void add_file_entry(String var1, PageId var2) throws FileNameTooLongException, InvalidPageNumberException, InvalidRunSizeException, DuplicateEntryException, OutOfSpaceException, FileIOException, IOException, DiskMgrException {
        if (var1.length() >= 50) {
            throw new FileNameTooLongException((Exception)null, "DB filename too long");
        } else if (var2.pid >= 0 && var2.pid < this.num_pages) {
            if (this.get_file_entry(var1) != null) {
                throw new DuplicateEntryException((Exception)null, "DB fileentry already exists");
            } else {
                Page var3 = new Page();
                boolean var4 = false;
                int var5 = 0;
                PageId var6 = new PageId();
                PageId var7 = new PageId(0);

                Object var8;
                do {
                    var6.pid = var7.pid;
                    this.pinPage(var6, var3, false);
                    if (var6.pid == 0) {
                        var8 = new DBFirstPage();
                        ((DBFirstPage)var8).openPage(var3);
                    } else {
                        var8 = new DBDirectoryPage();
                        ((DBDirectoryPage)var8).openPage(var3);
                    }

                    var7 = ((DBHeaderPage)var8).getNextPage();
                    int var9 = 0;

                    for(PageId var10 = new PageId(); var9 < ((DBHeaderPage)var8).getNumOfEntries(); ++var9) {
                        ((DBHeaderPage)var8).getFileEntry(var10, var9);
                        if (var10.pid == -1) {
                            break;
                        }
                    }

                    if (var9 < ((DBHeaderPage)var8).getNumOfEntries()) {
                        var5 = var9;
                        var4 = true;
                    } else if (var7.pid != -1) {
                        this.unpinPage(var6, false);
                    }
                } while(var7.pid != -1 && !var4);

                if (!var4) {
                    try {
                        this.allocate_page(var7);
                    } catch (Exception var11) {
                        this.unpinPage(var6, false);
                        var11.printStackTrace();
                    }

                    ((DBHeaderPage)var8).setNextPage(var7);
                    this.unpinPage(var6, true);
                    var6.pid = var7.pid;
                    this.pinPage(var6, var3, true);
                    var8 = new DBDirectoryPage(var3);
                    var5 = 0;
                }

                ((DBHeaderPage)var8).setFileEntry(var2, var1, var5);
                this.unpinPage(var6, true);
            }
        } else {
            throw new InvalidPageNumberException((Exception)null, " DB bad page number");
        }
    }

    public void delete_file_entry(String var1) throws FileEntryNotFoundException, IOException, FileIOException, InvalidPageNumberException, DiskMgrException {
        Page var2 = new Page();
        boolean var3 = false;
        int var4 = 0;
        PageId var5 = new PageId();
        PageId var6 = new PageId(0);
        PageId var7 = new PageId();

        Object var8;
        do {
            var5.pid = var6.pid;
            this.pinPage(var5, var2, false);
            if (var5.pid == 0) {
                var8 = new DBFirstPage();
                ((DBFirstPage)var8).openPage(var2);
            } else {
                var8 = new DBDirectoryPage();
                ((DBDirectoryPage)var8).openPage(var2);
            }

            var6 = ((DBHeaderPage)var8).getNextPage();

            int var9;
            for(var9 = 0; var9 < ((DBHeaderPage)var8).getNumOfEntries(); ++var9) {
                String var10 = ((DBHeaderPage)var8).getFileEntry(var7, var9);
                if (var7.pid != -1 && var10.compareTo(var1) == 0) {
                    break;
                }
            }

            if (var9 < ((DBHeaderPage)var8).getNumOfEntries()) {
                var4 = var9;
                var3 = true;
            } else {
                this.unpinPage(var5, false);
            }
        } while(var6.pid != -1 && !var3);

        if (!var3) {
            throw new FileEntryNotFoundException((Exception)null, "DB file not found");
        } else {
            var7.pid = -1;
            ((DBHeaderPage)var8).setFileEntry(var7, "\u0000", var4);
            this.unpinPage(var5, true);
        }
    }

    public PageId get_file_entry(String var1) throws IOException, FileIOException, InvalidPageNumberException, DiskMgrException {
        Page var2 = new Page();
        boolean var3 = false;
        int var4 = 0;
        PageId var5 = new PageId();
        PageId var6 = new PageId(0);

        Object var7;
        do {
            var5.pid = var6.pid;
            this.pinPage(var5, var2, false);
            if (var5.pid == 0) {
                var7 = new DBFirstPage();
                ((DBFirstPage)var7).openPage(var2);
            } else {
                var7 = new DBDirectoryPage();
                ((DBDirectoryPage)var7).openPage(var2);
            }

            var6 = ((DBHeaderPage)var7).getNextPage();
            int var8 = 0;

            for(PageId var9 = new PageId(); var8 < ((DBHeaderPage)var7).getNumOfEntries(); ++var8) {
                String var10 = ((DBHeaderPage)var7).getFileEntry(var9, var8);
                if (var9.pid != -1 && var10.compareTo(var1) == 0) {
                    break;
                }
            }

            if (var8 < ((DBHeaderPage)var7).getNumOfEntries()) {
                var4 = var8;
                var3 = true;
            }

            this.unpinPage(var5, false);
        } while(var6.pid != -1 && !var3);

        if (!var3) {
            return null;
        } else {
            PageId var11 = new PageId();
            ((DBHeaderPage)var7).getFileEntry(var11, var4);
            return var11;
        }
    }

    public String db_name() {
        return this.name;
    }

    public int db_num_pages() {
        return this.num_pages;
    }

    public int db_page_size() {
        return 1024;
    }

    public void dump_space_map() throws DiskMgrException, IOException, FileIOException, InvalidPageNumberException {
        System.out.println("********  IN DUMP");
        int var1 = (this.num_pages + 8192 - 1) / 8192;
        int var2 = 0;
        PageId var3 = new PageId();
        System.out.println("num_map_pages = " + var1);
        System.out.println("num_pages = " + this.num_pages);

        for(int var4 = 0; var4 < var1; ++var4) {
            var3.pid = 1 + var4;
            Page var5 = new Page();
            this.pinPage(var3, var5, false);
            int var6 = this.num_pages - var4 * 8192;
            System.out.println("num_bits_this_page = " + var6);
            System.out.println("num_pages = " + this.num_pages);
            if (var6 > 8192) {
                var6 = 8192;
            }

            int var7 = 0;

            for(byte[] var8 = var5.getpage(); var6 > 0; ++var7) {
                for(int var9 = 1; var9 < 256 && var6 > 0; ++var2) {
                    int var10 = var8[var7] & var9;
                    if (var2 % 10 == 0) {
                        if (var2 % 50 == 0) {
                            if (var2 > 0) {
                                System.out.println("\n");
                            }

                            System.out.print("\t" + var2 + ": ");
                        } else {
                            System.out.print(' ');
                        }
                    }

                    if (var10 != 0) {
                        System.out.print("1");
                    } else {
                        System.out.print("0");
                    }

                    var9 <<= 1;
                    --var6;
                }
            }

            this.unpinPage(var3, false);
        }

        System.out.println();
    }

    private void set_bits(PageId var1, int var2, int var3) throws InvalidPageNumberException, FileIOException, IOException, DiskMgrException {
        if (var1.pid >= 0 && var1.pid + var2 <= this.num_pages) {
            int var4 = var1.pid / 8192 + 1;
            int var5 = (var1.pid + var2 - 1) / 8192 + 1;
            int var6 = var1.pid % 8192;

            for(PageId var7 = new PageId(var4); var7.pid <= var5; var6 = 0) {
                Page var8 = new Page();
                this.pinPage(var7, var8, false);
                byte[] var9 = var8.getpage();
                int var10 = var6 / 8;
                int var11 = var6 % 8;
                int var12 = var6 + var2 - 1;
                if (var12 >= 8192) {
                    var12 = 8191;
                }

                int var13 = var12 / 8;

                for(int var14 = var10; var14 <= var13; var11 = 0) {
                    int var15 = 8 - var11;
                    int var16 = var2 > var15 ? var15 : var2;
                    int var17 = 1;
                    var17 = (var17 << var16) - 1 << var11;
                    Integer var19 = new Integer(var17);
                    Byte var20 = new Byte(var19.byteValue());
                    byte var21 = var20;
                    int var18;
                    if (var3 == 1) {
                        var18 = var9[var14] | var21;
                        var19 = new Integer(var18);
                        var9[var14] = var19.byteValue();
                    } else {
                        var18 = var9[var14] & (255 ^ var21);
                        var19 = new Integer(var18);
                        var9[var14] = var19.byteValue();
                    }

                    var2 -= var16;
                    ++var14;
                }

                this.unpinPage(var7, true);
                ++var7.pid;
            }

        } else {
            throw new InvalidPageNumberException((Exception)null, "Bad page number");
        }
    }

    private void pinPage(PageId var1, Page var2, boolean var3) throws DiskMgrException {
        try {
            SystemDefs.JavabaseBM.pinPage(var1, var2, var3);
        } catch (Exception var5) {
            throw new DiskMgrException(var5, "DB.java: pinPage() failed");
        }
    }

    private void unpinPage(PageId var1, boolean var2) throws DiskMgrException {
        try {
            SystemDefs.JavabaseBM.unpinPage(var1, var2);
        } catch (Exception var4) {
            throw new DiskMgrException(var4, "DB.java: unpinPage() failed");
        }
    }

}
