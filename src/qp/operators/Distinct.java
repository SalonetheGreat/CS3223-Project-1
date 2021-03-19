/**
 * To distinct out the required attributes from the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.Collections;

public class Distinct extends Operator {

    Operator base;                 // Base table to distinct
    int numBuff;                   // Number of buffer available
    int batchsize;                 // Number of tuples per outbatch
    ArrayList<Attribute> attList;
    ExternalSort externalSort;
    ArrayList<Integer> indexList;
    /**
     * The following fields are requied during execution
     * * of the Distinct Operator
     **/
    Batch inbatch;
    Batch outbatch;


    /**
     * index of the attributes in the base operator
     * * that are to be distinct
     **/
    int[] attrIndex;

    public Distinct(Operator base, int type) {
        super(type);
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public int getNumBuff() {
        return numBuff;
    }

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    /**
     * Opens the connection to the base operator
     * * Also prepare the external sort
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;
        attList = new ArrayList<>();
        attList = base.getSchema().getAttList();
        indexList = new ArrayList<Integer>(0);
        for (int i=0; i < attList.size(); ++i) {
            indexList.add(i+1);
        }
        externalSort = new ExternalSort(OpType.SORT, base, indexList, numBuff);
        if (!externalSort.open()) return false;

        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/
        Tuple previousTuple = null;

        if (inbatch == null) {
            return null;
        }

        while (!outbatch.isFull()) {
            for (int i = 0; i < numBuff; ++i) {
                inbatch = externalSort.next();
                for (int curs = 0; curs < inbatch.size(); ++curs) {
                    Tuple currTuple = inbatch.get(curs);
                    if (previousTuple == null) {
                        previousTuple = currTuple;
                    }
                    if (previousTuple != currTuple) {
                        outbatch.add(currTuple);
                        previousTuple = currTuple;
                    }
                }
                if (inbatch == null) {
                    break;
                }
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        if (!externalSort.close()) return false;
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Distinct newDistinct = new Distinct(newbase, optype);
        newDistinct.setNumBuff(numBuff);
        return newDistinct;
    }

}
