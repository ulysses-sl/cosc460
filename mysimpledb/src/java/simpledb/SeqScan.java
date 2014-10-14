package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class  SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    
    int tabId;
    String alias;
    DbFileIterator it;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	tabId = tableid;
    	alias = tableAlias;
    	it = Database.getCatalog().getDatabaseFile(tabId).iterator(tid);
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
    	return Database.getCatalog().nameById.get(tabId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
    	return alias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	TupleDesc td = Database.getCatalog().getTupleDesc(tabId);
    	int size = td.getSize();
    	Type[] typeAr = new Type[size];
    	String[] fieldAr = new String[size];
    	
    	Iterator<TupleDesc.TDItem> tdit = td.iterator();
    	int i = 0;
    	while (tdit.hasNext()) {
    		TupleDesc.TDItem tditem = tdit.next();
    		typeAr[i] = tditem.fieldType;
    		fieldAr[i] = getAlias() + "." + tditem.fieldName;
    		i++;
    	}
        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return it.next();
    }

    public void close() {
        it.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        it.rewind();
    }
}
