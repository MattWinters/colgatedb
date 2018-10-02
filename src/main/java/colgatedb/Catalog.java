package colgatedb;

import colgatedb.dbfile.DbFile;
import colgatedb.dbfile.HeapFile;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */
public class Catalog {

    private final DiskManagerImpl dm;
    private final int pageSize;

    private final Map<Integer, TableInfo> id2info;
    private final HashMap<String, Integer> name2id;

    /**
     * TableInfo data structure used to hold information about each table.
     */
    private class TableInfo {
        String name;
        DbFile table;
        String key;
        File file;   // not strictly necessary, but useful for testing

        public TableInfo(String name) {
            this.name = name;
        }
    }

    /**
     * Constructor. Creates a new, empty catalog.
     */
    public Catalog(int pageSize, DiskManagerImpl dm) {
        this.pageSize = pageSize;
        this.dm = dm;
        id2info = new HashMap<Integer, TableInfo>();
        name2id = new HashMap<String, Integer>();
    }

    public void addTable(String name, DbFile table, String primaryKey, File dataFile) {
        if (dataFile == null || !dataFile.exists()) {
            throw new CatalogException("Invalid file object.");
        }
        int id = table.getId();
        TableInfo info = new TableInfo(name);
        info.file = dataFile;
        info.table = table;
        info.key = primaryKey;
        id2info.put(id, info);
        name2id.put(name, id);
//        dm.addFileEntry(id, dataFile.getAbsolutePath());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        if (!name2id.containsKey(name)) {
            throw new NoSuchElementException();
        }
        return name2id.get(name);
    }

    public String getTableName(int id) {
        checkId(id);
        return id2info.get(id).name;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        checkId(tableid);
        return id2info.get(tableid).table.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        checkId(tableid);
        return id2info.get(tableid).table;
    }

    public String getPrimaryKey(int tableid) {
        checkId(tableid);
        return id2info.get(tableid).key;
    }

    private void checkId(int tableid) throws NoSuchElementException {
        if (!id2info.containsKey(tableid)){
            throw new NoSuchElementException();
        }
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        id2info.clear();
        name2id.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * The catalog file is a plain text file with a format like this:

     Actor(id int pk, fname string, lname string, gender string)
     Movie(id int pk, name string, year int)
     Director(id int pk, fname string, lname string)
     Casts(pid int, mid int, role string)
     Movie_Director(did int, mid int)
     Genre(mid int, genre string)

     * in the above, "pk" indicates that the field is the primary key for that table.
     *
     * This implementation assumes that (a) each table is stored in a separate file whose name is the name
     * of the table followed by ".dat" and (b) is located in the same directory as catalogFile and (c) each
     * table is stored in HeapFile format.
     *
     * @param catalogFile an existing catalog file
     */
    public void loadSchema(File catalogFile) {
        String line = "";
        File baseFolder = new File(new File(catalogFile.getAbsolutePath()).getParent());
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[types.size()]);
                String[] namesAr = names.toArray(new String[names.size()]);
                TupleDesc td = new TupleDesc(typeAr, namesAr);
                File dataFile = new File(baseFolder, name + ".dat");
                HeapFile tabHf = addHeapFile(name, td, primaryKey, dataFile);
                System.out.print("Added table : " + name + " with schema " + td + (primaryKey.equals("") ? "" : (" key is " + primaryKey)));
                System.out.println(" Table has " + dm.getNumPages(tabHf.getId()) + " pages.");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            throw new RuntimeException(e);
        }
    }

    public void loadSchema(String filename) {
        loadSchema(new File(filename));
    }

    // used in testing
    public File getFile(int tableid) {
        return id2info.get(tableid).file;
    }

    // needed for tablestats
    public Iterator<Integer> tableIdIterator() {
        return id2info.keySet().iterator();
    }

    public static HeapFile addHeapFile(String name, TupleDesc td, File dataFile) {
        return addHeapFile(name, td, "", dataFile);
    }

    public static HeapFile addHeapFile(String name, TupleDesc td, String primaryKey, File dataFile) {
        int tableid = tableIdForFile(dataFile);
        Database.getDiskManager().addFileEntry(tableid, dataFile.getAbsolutePath());
        HeapFile hf = new HeapFile(td, Database.getPageSize(), tableid, Database.getDiskManager().getNumPages(tableid));
        Database.getCatalog().addTable(name, hf, primaryKey, dataFile);
        return hf;
    }

    private static int tableIdForFile(File tableFile) {
        assert tableFile.exists();
        return tableFile.getAbsolutePath().hashCode();
    }
}

