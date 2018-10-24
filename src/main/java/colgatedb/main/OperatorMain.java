package colgatedb.main;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.operators.*;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */
public class OperatorMain {

    public static void main(String[] argv)
            throws DbException, TransactionAbortedException, IOException {

        // file named college.schema must be in colgatedb directory
        String filename = "college.schema";
        System.out.println("Loading schema from file: " + filename);
        Database.getCatalog().loadSchema(filename);

        // SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
        // algebra translation: select_{name="alice"}( Students )
        // query plan: a tree with the following structure
        // - a Filter operator is the root; filter keeps only those w/ name=Alice
        // - a SeqScan operator on Students at the child of root
        TransactionId tid = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("Students"));
        StringField alice = new StringField("alice", Type.STRING_LEN);

        Predicate p = new Predicate(1, Op.EQUALS, alice);
        DbIterator filterStudents = new Filter(p, scanStudents);

        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        filterStudents.open();
        while (filterStudents.hasNext()) {
            Tuple tup = filterStudents.next();
            System.out.println("\t" + tup);
        }
        filterStudents.close();


        tid = new TransactionId();
        SeqScan Takes = new SeqScan(tid, Database.getCatalog().getTableId("Takes"));
        SeqScan Professors = new SeqScan(tid, Database.getCatalog().getTableId("Profs"));
        //IntField course = new IntField("cid", Type.INT_TYPE);
        //IntField favoriteCourse = new IntField("favoriteCourse", Type.INT_TYPE);
        JoinPredicate sidEq = new JoinPredicate(0, Op.EQUALS, 0);
        DbIterator STjoin = new Join(sidEq, scanStudents, Takes);

        //#################
        // Print out TPjoin !!!!!!!!!


        //

        tid = new TransactionId();
        StringField hay = new StringField("hay", Type.STRING_LEN);
        Predicate namneEqHay = new Predicate(1, Op.EQUALS, hay);
        DbIterator profsNamedHay = new Filter(namneEqHay, Professors);
//############### Field number needs to be whatever the field corresponds too in STjoin#########
        JoinPredicate cidEq = new JoinPredicate(3, Op.EQUALS, 2);
        DbIterator CIDjoin = new Join(cidEq, STjoin, profsNamedHay);

//        System.out.println("Query results:");
//        CIDjoin.open();
//        Tuple t;
//        while (CIDjoin.hasNext()) {
//            t = CIDjoin.next();
//            System.out.println("\t"+ t);
//        }
//        CIDjoin.close();

        ArrayList<Integer> fieldList = new ArrayList<Integer>();
        fieldList.add(1);
        Type[] types = new Type[]{Type.STRING_TYPE};
        DbIterator names = new Project(fieldList, types, CIDjoin);

        System.out.println("Query results:");
        names.open();
        Tuple t2;
        while (names.hasNext()) {
            t2 = names.next();
            System.out.println("\t" + t2);
        }
        names.close();
    }

}