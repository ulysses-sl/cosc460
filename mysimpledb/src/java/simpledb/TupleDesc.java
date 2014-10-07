package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
    ArrayList<TDItem> desc;

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	desc = new ArrayList<TDItem>();
    	int descLen = typeAr.length;
    	for (int i = 0; i < descLen; i++) {
            if (typeAr[i] != null) {
    		    desc.add(new TDItem(typeAr[i], fieldAr[i]));
            }
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	desc = new ArrayList<TDItem>();
    	int descLen = typeAr.length;
    	for (int i = 0; i < descLen; i++) {
            if (typeAr[i] != null) {
    		    desc.add(new TDItem(typeAr[i], ""));
            }
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return desc.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if (i < 0 || i >= numFields()) {
    		throw new NoSuchElementException();
    	}
        return desc.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if (i < 0 || i >= numFields()) {
    		throw new NoSuchElementException();
    	}
        return desc.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	int i = 0;
    	for (TDItem td : desc) {
    		if (td.fieldName.equals(name)) {
    			return i;
    		}
    		i++;
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int len = 0;
    	for (TDItem i : desc) {
    		len += i.fieldType.getLen();
    	}
    	return len;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	Type[] temp = {null};
    	TupleDesc td3 = new TupleDesc(temp);
    	td3.desc = new ArrayList<TDItem>();
    	td3.desc.addAll(td1.desc);
    	td3.desc.addAll(td2.desc);
        return td3;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (!(o instanceof TupleDesc)) {
    		return false;
    	}
        if (getSize() == ((TupleDesc) o).getSize()) {
        	Iterator<TDItem> it1 = desc.iterator();
        	Iterator<TDItem> it2 = ((TupleDesc) o).desc.iterator();
        	while (it1.hasNext() && it2.hasNext()) {
        		if (it1.next().fieldType != it2.next().fieldType) {
        			return false;
        		}
        	}
        	return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])"
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String str = "";
        boolean iReallyWishICouldUseSomeMapAndReduce = false;
        for (TDItem i : desc) {
        	if (iReallyWishICouldUseSomeMapAndReduce) {
        		str = str.concat(", ");
        	}
        	else {
                iReallyWishICouldUseSomeMapAndReduce = true;
            }
        	str = str.concat(i.fieldName + "(" + i.fieldType.toString() + ")");
        }
        return str;
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return desc.iterator();
    }

}
