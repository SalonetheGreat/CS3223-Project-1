/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import org.w3c.dom.Attr;
import qp.utils.Aggregation;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch
    boolean hasAggregation;        //whether to project aggregation or not

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;

    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        hasAggregation = false;

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            if (attr.getAggType() != Attribute.NONE && hasAggregation == false) {
                hasAggregation = true;
            } else if (hasAggregation == true && attr.getAggType() == Attribute.NONE) {
                    System.err.println("Cannot select aggregation and tuples at the same time.");
                    System.exit(1);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
        }
        return true;
    }

    public Batch next() {
        if (hasAggregation) {
            return next_aggregation();
        } else {
            return next_noaggregation();
        }
    }

    public Batch next_aggregation() {
        outbatch = new Batch(batchsize);
        inbatch = base.next();
        if (inbatch == null) {
            return null;
        }
        ArrayList<Aggregation> aggregations = new ArrayList<>();

        while (inbatch != null) {
            for (int i = 0; i < inbatch.size(); ++i) {
                Tuple basetuple = inbatch.get(i);
                for (int j = 0; j < attrset.size(); ++j) {
                    Object data = basetuple.dataAt(attrIndex[j]);
                    if (aggregations.size() < attrset.size()) {
                        /**System.out.println("attr name: " + attrset.get(j).getColName());
                        System.out.println("attr type: " + attrset.get(j).getType());**/
                        int type;
                        if (data instanceof Integer) {
                            attrset.get(j).setType(Attribute.INT);
                            type = Attribute.INT;
                        } else if (data instanceof Float) {
                            attrset.get(j).setType(Attribute.REAL);
                            type =Attribute.REAL;
                        } else {
                            attrset.get(j).setType(Attribute.STRING);
                            type = Attribute.STRING;
                        }
                        aggregations.add(new Aggregation(data, type));
                        //System.out.println("aggregations size: " + aggregations.size());
                        continue;
                    }
                    aggregations.get(j).update(data);
                }
            }
            inbatch = base.next();
        }
        //System.out.println("make new tuple");
        //System.out.println("max: " + aggregations.get(0).returnMaxInt());
        ArrayList<Object> present = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);
            //System.out.println("aggregate type: " + attr.getAggType());
            if (attr.getAggType() == Attribute.MIN) {
                if (attr.getType() == Attribute.INT) {
                    present.add(aggregations.get(i).returnMinInt());
                } else if (attr.getType() == Attribute.REAL){
                    present.add(aggregations.get(i).returnMinFlt());
                } else {
                    present.add(aggregations.get(i).returnMinStr());
                }
            } else if (attr.getAggType() == Attribute.MAX) {
                //System.out.println("Doing MAX");
                if (attr.getType() == Attribute.INT) {
                   // System.out.println("max: " + aggregations.get(i).returnMaxInt());
                    present.add(aggregations.get(i).returnMaxInt());
                } else if (attr.getType() == Attribute.REAL){
                    present.add(aggregations.get(i).returnMaxFlt());
                } else {
                    present.add(aggregations.get(i).returnMaxStr());
                }
            } else if (attr.getAggType() == Attribute.COUNT) {
                present.add(aggregations.get(i).returnCount());
            } else {
                present.add(aggregations.get(i).returnAvg());
            }
        }
        Tuple outtuple = new Tuple(present);
        outbatch.add(outtuple);
        return outbatch;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next_noaggregation() {
        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/
        inbatch = base.next();

        if (inbatch == null) {
            return null;
        }

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            //Debug.PPrint(basetuple);
            //System.out.println();
            ArrayList<Object> present = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                Object data = basetuple.dataAt(attrIndex[j]);
                present.add(data);
            }
            Tuple outtuple = new Tuple(present);
            outbatch.add(outtuple);
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}
