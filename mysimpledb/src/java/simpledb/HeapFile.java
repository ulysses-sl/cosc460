package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
	TupleDesc tDesc;
	File file;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
    	tDesc = td;
    	file = f;
    	Database.getCatalog().addTable(this);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
    	return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return tDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	int pageNo = pid.pageNumber();
    	int pagesize = BufferPool.getPageSize();
    	byte[] page = new byte[pagesize];
    	try {
    	    InputStream inputstream = new FileInputStream(file);
    	    inputstream.skip(pagesize * pageNo);
    	
            byte data = (byte) inputstream.read();
    	    for (int i = 0; i < pagesize; i++) {
       		    page[i] = data;
    	        data = (byte)inputstream.read();
    	    }
    	    inputstream.close();
    	    return new HeapPage((HeapPageId) pid, page);
        } catch (IOException e) {
    		System.out.println("file not accessible");
    	}
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	int pageNo = page.getId().pageNumber();
    	int pagesize = BufferPool.getPageSize();
        RandomAccessFile dbfile = new RandomAccessFile(file, "rw");
        //System.out.println(pageNo);
        dbfile.seek(pagesize * pageNo);
        dbfile.write(page.getPageData());
        dbfile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	HeapPage page;
    	try {
    		int pageno = t.getRecordId().getPageId().pageNumber();
    		if (pageno >= numPages()) {
    			pageno = numPages() - 1;
    		}
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageno), null);
    	} catch (NoSuchElementException e) {
    		System.out.println(numPages());
    		page = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
    	}
    	if (page.getNumEmptySlots() == 0) {
    		page = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
    	}
    	page.insertTuple(t);
    	writePage(page);
    	ArrayList<Page> listOfPage = new ArrayList<Page>();
    	listOfPage.add(page);
        return listOfPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	HeapPage page;
    	try {
    	    page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), null);
    	} catch (NoSuchElementException e) {
    		throw new DbException("No such page");
    	}
    	page.deleteTuple(t);
    	ArrayList<Page> listOfPage = new ArrayList<Page>();
    	listOfPage.add(page);
        return listOfPage;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new PageIterator();
    }

    class PageIterator implements DbFileIterator {
    	private boolean openYet;
    	private int currentPageNo;
    	private Iterator<Tuple> currentPageIterator;
    	
    	public PageIterator() {
    		openYet = false;
    		currentPageNo = 0;
    	}
    /**
     * Opens the iterator
     *
     * @throws DbException when there are problems opening/accessing the database.
     */
		@Override
        public void open() throws DbException, TransactionAbortedException{
			currentPageIterator = ((HeapPage) Database.getBufferPool().getPage(null, new HeapPageId(getId(), currentPageNo), null)).iterator();
			currentPageNo++;
			openYet = true;
		}

    /**
     * @return true if there are more tuples available.
     */
		@Override
        public boolean hasNext() {
			return openYet && (currentPageNo < numPages() || currentPageIterator.hasNext());
		}

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
		@Override
        public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			else if (currentPageIterator.hasNext()) {
				return currentPageIterator.next();
			}
			else if (currentPageNo < numPages()) {
                currentPageIterator = ((HeapPage) Database.getBufferPool().getPage(null, new HeapPageId(getId(), currentPageNo), null)).iterator();
				currentPageNo++;
				return currentPageIterator.next();
			}
			else {
				throw new NoSuchElementException();
			}
		}

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException When rewind is unsupported.
     */
		@Override
        public void rewind() throws DbException, TransactionAbortedException {
			currentPageNo = 0;
			open();
		}

    /**
     * Closes the iterator.
     */
		@Override
        public void close() {
			openYet = false;
		}

    }
}

