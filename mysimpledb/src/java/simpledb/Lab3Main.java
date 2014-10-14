package simpledb;

import java.io.IOException;
import java.util.ArrayList;

public class Lab3Main {

    public static void main(String[] argv)
            throws DbException, TransactionAbortedException, IOException {

        System.out.println("Loading schema from file:");
        // file named college.schema must be in mysimpledb directory
        Database.getCatalog().loadSchema("college.schema");

        // SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
        // algebra translation: select_{name="alice"}( Students )
        // query plan: a tree with the following structure
        // - a Filter operator is the root; filter keeps only those w/ name=Alice
        // - a SeqScan operator on Students at the child of root
        TransactionId tid = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("Students"));
        SeqScan scanCourses = new SeqScan(tid, Database.getCatalog().getTableId("Courses"));
        SeqScan scanProfs = new SeqScan(tid, Database.getCatalog().getTableId("Profs"));
        SeqScan scanTakes = new SeqScan(tid, Database.getCatalog().getTableId("Takes"));

        StringField alice = new StringField("alice", Type.STRING_LEN);
        Predicate p = new Predicate(1, Predicate.Op.EQUALS, alice);
        Filter filterStudents = new Filter(p, scanStudents);

        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        filterStudents.open();
        while (filterStudents.hasNext()) {
            Tuple tup = filterStudents.next();
            System.out.println("\t"+tup);
        }
        filterStudents.close();

        int cidNum = scanCourses.getTupleDesc().fieldNameToIndex("Courses.cid");
        int favNum = scanProfs.getTupleDesc().fieldNameToIndex("Profs.favoriteCourse");
        Join joinFavCourse = new Join(new JoinPredicate(favNum, Predicate.Op.EQUALS, cidNum), scanProfs, scanCourses);
        System.out.println("\nQuery results:");
        joinFavCourse.open();
        while (joinFavCourse.hasNext()) {
            Tuple tup = joinFavCourse.next();
            System.out.println("\t"+tup);
        }
        joinFavCourse.close();

        scanStudents.rewind();
        int ssid = scanStudents.getTupleDesc().fieldNameToIndex("Students.sid");
        int tsid = scanTakes.getTupleDesc().fieldNameToIndex("Takes.sid");
        Join joinSid = new Join(new JoinPredicate(ssid, Predicate.Op.EQUALS, tsid), scanStudents, scanTakes);
        System.out.println("\nQuery results:");
        joinSid.open();
        while (joinSid.hasNext()) {
            Tuple tup = joinSid.next();
            System.out.println("\t"+tup);
        }
        joinSid.close();

        scanStudents.rewind();
        scanTakes.rewind();
        scanProfs.rewind();

        Join joinSid2 = new Join(new JoinPredicate(ssid, Predicate.Op.EQUALS, tsid), scanStudents, scanTakes);

        int tcid = joinSid2.getTupleDesc().fieldNameToIndex("Takes.cid");
        int pcid = scanProfs.getTupleDesc().fieldNameToIndex("Profs.favoriteCourse");
        Join joinCid2 = new Join(new JoinPredicate(tcid, Predicate.Op.EQUALS, pcid), joinSid2, scanProfs);

        int pname = joinCid2.getTupleDesc().fieldNameToIndex("Profs.name");
        Filter filterName = new Filter(new Predicate(pname, Predicate.Op.EQUALS, new StringField("Hay", 5)), joinCid2);

        int sname = filterName.getTupleDesc().fieldNameToIndex("Students.name");

        ArrayList<Integer> fieldList = new ArrayList<Integer>();
        fieldList.add(sname);
        Type[] typesList = { Type.STRING_TYPE };
        Project projectName = new Project(fieldList, typesList, filterName);

        System.out.println("\nQuery results:");
        projectName.open();
        while (projectName.hasNext()) {
            Tuple tup = projectName.next();
            System.out.println("\t"+tup);
        }
        projectName.close();

        Database.getBufferPool().transactionComplete(tid);
    }

}