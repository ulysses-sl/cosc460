package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    TransactionId tid;
    DbIterator childIt;
    int tabid;
    boolean isopen;
    boolean inserted;
    TupleDesc td;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        tid = t;
        childIt = child;
        tabid = tableid;
        isopen = false;
        inserted = false;
        Type[] inttype = { Type.INT_TYPE };
        td = new TupleDesc(inttype);
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        try {
            childIt.open();
        } catch (DbException e) {
            throw new DbException("could not open");
        }
        isopen = true;
        rewind();
    }

    public void close() {
        childIt.close();
        super.close();
        isopen = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        if (!isopen) {
            throw new DbException("not open yet");
        }
        childIt.rewind();
        inserted = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (inserted) {
            return null;
        }
        Tuple tup = new Tuple(td);
        int num = 0;
        while (childIt.hasNext()) {
            try {
                Tuple tpl = childIt.next();
                if (tpl == null) { throw new DbException("poop 1"); }
                if (tpl.getRecordId() == null) { throw new DbException("poop 2"); }
                if (tpl.getRecordId().getPageId() == null) { throw new DbException("poop 3"); }
                Database.getBufferPool().insertTuple(tid, tabid, tpl);
                num++;
            } catch (IOException e) {
                throw new DbException("could not insert tuples");
            }
        }
        tup.setField(0, new IntField(num));
        inserted = true;
        return tup;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] ret = {childIt};
        return ret;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        childIt = children[0];
    }
}
