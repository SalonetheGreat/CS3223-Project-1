/**
 * To order the required attributes from the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Orderby extends Operator {

    Operator base;                 // Base table to distinct
    int numBuff;                   // Number of buffer available
    int batchsize;                 // Number of tuples per outbatch
    boolean isDes;
    ArrayList<Attribute> orderbyList;
    ExternalSort externalSort;

    /**
     * The following fields are requied during execution
     * * of the Distinct Operator
     **/
    Batch inbatch;
    Batch outbatch;


    /**
     * index of the attributes in the base operator
     * * that are to be ordered
     **/
    ArrayList<Integer> attrIndex;

    public Orderby(Operator base, ArrayList<Attribute> orderbyList, boolean isDes, int type) {
        super(type);
        this.orderbyList = orderbyList;
        this.isDes = isDes;
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

        Schema baseSchema = base.getSchema();
        attrIndex = new ArrayList<Integer>(orderbyList.size());
        for (int i = 0; i < orderbyList.size(); ++i) {
            Attribute attr = orderbyList.get(i);
            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex.add(index);
        }

        //need to change sorting algorithm here to include attrIndex
        //isDes is used here
        externalSort = new ExternalSort(OpType.SORT, base, attrIndex, numBuff);
        if (!externalSort.open()) return false;

        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {
            inbatch = externalSort.next();
            for (int curs = 0; curs < inbatch.size(); ++curs) {
                Tuple currTuple = inbatch.get(curs);
                outbatch.add(currTuple);
            }
            if (inbatch == null) {
                break;
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
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < orderbyList.size(); ++i)
            newattr.add((Attribute) orderbyList.get(i).clone());
        Boolean newDsc = isDes;
        Orderby newOrderby = new Orderby(newbase, newattr, newDsc, optype);
        newOrderby.setNumBuff(numBuff);
        return newOrderby;
    }

}
