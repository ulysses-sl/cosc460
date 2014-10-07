package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    TransactionId tid;
    DbIterator childIt;
    boolean isopen, deleted;
    TupleDesc td;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        tid = t;
        childIt = child;
        isopen = false;
        deleted = false;
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
        deleted = false;
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
        deleted = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (deleted) {
            return null;
        }
        Tuple tup = new Tuple(td);
        int num = 0;
        while (childIt.hasNext()) {
            try {
                Tuple tpl = childIt.next();
                Database.getBufferPool().deleteTuple(tid, tpl);
                num++;
            } catch (IOException e) {
                throw new DbException("could not delete tuples");
            }
        }
        tup.setField(0, new IntField(num));
        deleted = true;
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
