package simpledb;

import java.io.*;

import java.util.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p/>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    
    ConcurrentHashMap<PageId, Page> buffer;
    ConcurrentHashMap<PageId, Long> accessTime;
    int maxPages;
    
    public BufferPool(int numPages) {
    	buffer = new ConcurrentHashMap<PageId, Page>();
        accessTime = new ConcurrentHashMap<PageId, Long>();
    	maxPages = numPages;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p/>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        if (buffer.containsKey(pid)) {
            accessTime.put(pid, System.currentTimeMillis());
        	return buffer.get(pid);
        }
        if (buffer.size() >= maxPages) {
            evictPage();
        }
        Catalog cat = Database.getCatalog();
        Page newPage = cat.getDatabaseFile(pid.getTableId()).readPage(pid);
        buffer.put(pid, newPage);
        accessTime.put(pid, System.currentTimeMillis());
        return newPage;
        /*
        else {
        	evictPage();//throw new DbException("Buffer Full.");
        }*/
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed until lab5).                                  // cosc460
     * May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = file.insertTuple(tid, t);

        for (Page p : dirtyPages) {
            p.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = file.deleteTuple(tid, t);

        for (Page p : dirtyPages) {
            p.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        Catalog cat = Database.getCatalog();
        BufferPool buf = Database.getBufferPool();
        for (PageId pid : buf.buffer.keySet()) {
            Page page = null;
            try {
                page = buf.getPage(null, pid, null);
            } catch (TransactionAbortedException e) {
                System.out.println("Transaction aborted");
                e.printStackTrace();
            } catch (DbException e) {
                System.out.println("Database error");
                e.printStackTrace();
            }
            if (page.isDirty() != null) {
                cat.getDatabaseFile(pid.getTableId()).writePage(page);
                page.markDirty(false, null);
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab6                                                                            // cosc460
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Catalog cat = Database.getCatalog();
        BufferPool buf = Database.getBufferPool();
        Page page = null;
        try {
            page = buf.getPage(null, pid, null);
        } catch (TransactionAbortedException e) {
            System.out.println("Transaction aborted");
            e.printStackTrace();
        } catch (DbException e) {
            System.out.println("Database error");
            e.printStackTrace();
        }
        if (page.isDirty() != null) {
            cat.getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId pid = null;
        long oldest = 0;
        Iterator it = accessTime.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<PageId, Long> pairs = (Map.Entry<PageId, Long>) it.next();
            if ((oldest == 0) || (pairs.getValue() < oldest)) {
                pid = pairs.getKey();
                oldest = pairs.getValue();
            }
        }
        if (pid == null) {
            throw new DbException("no page to evict");
        }
        accessTime.remove(pid);
        try {
            flushPage(pid);
        } catch (IOException e) {
            System.out.println("IO exception");
            e.printStackTrace();
        }
        buffer.remove(pid);
    }

}
