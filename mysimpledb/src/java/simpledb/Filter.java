package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */

    Predicate pred;
    DbIterator childit;
    boolean isOpen;
    public Filter(Predicate p, DbIterator child) {
        pred = p;
        childit = child;
        isOpen = false;
    }

    public Predicate getPredicate() {
        return pred;
    }

    public TupleDesc getTupleDesc() {
        return childit.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        try {
            childit.open();
        } catch (DbException e) {
            throw new DbException("could not open");
        }
        isOpen = true;
    }

    public void close() {
        childit.close();
        super.close();
        isOpen = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        if (isOpen) {
            childit.rewind();
        }
        else {
            throw new DbException("open the iterator first");
        }
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (isOpen) {
            while (childit.hasNext()) {
                Tuple t = childit.next();
                if (pred.filter(t)) {
                    return t;
                }
            }
            return null;
        }
        else {
            throw new DbException("open iterator first");
        }
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] rv = { childit };
        return rv;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        childit = children[0];
    }

}
