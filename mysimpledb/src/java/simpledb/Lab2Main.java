package simpledb;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType;

import java.io.*;
/**
 * Created by sak on 9/29/14.
 */
public class Lab2Main {

    public static void main(String[] argv) {

        // construct a 3-column table schema
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {
            // and run it
            f.open();
            TupleDesc td;
            while (f.hasNext()) {
                Tuple tup = f.next();
                td = tup.getTupleDesc();
                IntField field1 = (IntField) tup.getField(1);
                if (field1.getValue() < 3) {
                    System.out.print("Update tuple: " + tup + " to be: ");
                    table1.deleteTuple(tid, tup);
                    tup.setField(1, new IntField(3));
                    System.out.println(tup.toString());
                    table1.insertTuple(tid, tup);
                }
            }

            Tuple newtup = Utility.getHeapTuple(99, 3);
            System.out.println("Insert tuples: " + newtup);
            table1.insertTuple(tid, newtup);

            System.out.println("The table now contains the following records");

            f.rewind();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println("Tuple: " + tup);
            }

            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println("Exception : " + e);
        }

    }
}