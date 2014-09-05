package simpledb;

import java.io.Serializable;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    TupleDesc desc_;
    Field[] fields_;
    RecordId rid_;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        desc_ = td;
        fields_ = new Field[desc_.getSize()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return desc_;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        return rid_;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
    	rid_ = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
    	String type1 = f.getType().toString();
    	String type2 = desc_.desc.get(i).fieldType.toString();
    	if (type1.equals(type2)) {
            fields_[i] = f;
    	}
    	else {
    		throw new RuntimeException();
    	}
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        return fields_[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p/>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p/>
     * where \t is any whitespace, except newline
     */
    public String toString() {
        String str = "";
        boolean iReallyWishICouldUseSomeMapAndReduce = false;
        for (Field f : fields_) {
        	if (iReallyWishICouldUseSomeMapAndReduce) {
        		str = str.concat(" ");
        	}
        	else {
                iReallyWishICouldUseSomeMapAndReduce = true;
            }
        	if (f != null) {
        	    str = str.concat(f.toString());
        	}
        }
        return str;
    }

}
