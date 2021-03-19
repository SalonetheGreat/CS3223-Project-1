/**
 * Sort Merge Join Algorithm
 */

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.util.ArrayList;

public class SortMergeJoin extends Join {

    int batchsize;                  // Number of tuples per out batch
    int leftbatchsize;
    int rightbatchsize;
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    Batch outbatch;                 // Buffer page for output
    ArrayList<Batch> leftbatches;
    ArrayList<Batch> rightbatches;

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    int lpgcurs;
    int rpgcurs;
    boolean eosl;
    boolean eosr;

    ExternalSort leftES;
    ExternalSort rightES;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        leftbatchsize = Batch.getPageSize() / left.getSchema().getTupleSize();
        rightbatchsize = Batch.getPageSize() / right.getSchema().getTupleSize();

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

        leftES = new ExternalSort(OpType.SORT, left, leftindex.get(0), numBuff);
        rightES = new ExternalSort(OpType.SORT, right, rightindex.get(0), numBuff);

        if (!leftES.open()) return false;
        if (!rightES.open()) return false;

        if (numBuff % 2 == 0) {
            leftbatches = new ArrayList<>(numBuff/2);
            rightbatches = new ArrayList<>(numBuff/2 -1);
        } else {
            leftbatches = new ArrayList<>(numBuff/2);
            rightbatches = new ArrayList<>(numBuff/2);
        }
        lpgcurs = 0;
        rpgcurs = 0;
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        eosr = false;
        return true;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (lpgcurs == leftbatches.size()) {
                for (int i = 0; i < leftbatches.size(); ++i) {
                    Batch curr = leftES.next();
                    leftbatches.set(i, curr);
                    if (curr == null) {
                        break;
                    }
                }
                lpgcurs = 0; lcurs = 0;
            }
            if (rpgcurs == rightbatches.size()) {
                for (int i = 0; i < rightbatches.size(); ++i) {
                    Batch curr = rightES.next();
                    rightbatches.set(i, curr);
                    if (curr == null) {
                        break;
                    }
                }
                rpgcurs = 0; rcurs = 0;
            }

            if (leftbatches.get(lpgcurs) == null) eosl = true;
            if (rightbatches.get(rpgcurs) == null) eosr = true;

            if (eosl || eosr) {
                if (outbatch.size() == 0) return null;
                else return outbatch;
            } else {
                Tuple lefttuple = leftbatches.get(lpgcurs).get(lcurs);
                Tuple righttuple = rightbatches.get(rpgcurs).get(rcurs);
                if (Tuple.compareTuples(lefttuple, righttuple, leftindex.get(0), rightindex.get(0)) == 0) {
                    Tuple outTuple = lefttuple.joinWith(righttuple);
                    outbatch.add(outTuple);
                    lcurs++; rcurs++;
                    if (lcurs == leftbatches.get(lpgcurs).size()) {
                        lcurs = 0;
                        lpgcurs++;
                    }
                    if (rcurs == rightbatches.get(rpgcurs).size()) {
                        rcurs = 0;
                        rpgcurs++;
                    }
                } else if (Tuple.compareTuples(lefttuple, righttuple, leftindex.get(0), rightindex.get(0)) < 0) {
                    lcurs++;
                    if (lcurs == leftbatches.get(lpgcurs).size()) {
                        lcurs = 0;
                        lpgcurs++;
                    }
                } else {
                    rcurs++;
                    if (rcurs == rightbatches.get(rpgcurs).size()) {
                        rcurs = 0;
                        rpgcurs++;
                    }
                }
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        if (!leftES.close()) return false;
        if (!rightES.close()) return false;
        return true;
    }

}
