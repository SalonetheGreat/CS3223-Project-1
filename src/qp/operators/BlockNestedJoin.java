package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    ArrayList<Batch> leftbatches;   // Buffer block for left input stream
    Batch leftbatch;               // Buffer page for left input stream
    int leftbatchsize;              //Number of tuples of left batch
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int lpgcurs;                    // Cursor for left block
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
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

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lpgcurs = 0;
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "NJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("NestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    public Batch next() {
        int i, j, k, l;
        if (eosl && lpgcurs == 0) {
            return null;
        }
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (lpgcurs == 0 && lcurs == 0 && eosr == true) {
                /** new left block needs to be fetched**/
                leftbatches = new ArrayList<Batch>();
                for (k = 0; k < numBuff-2; ++k) {
                    leftbatch = (Batch) left.next();
                    if (leftbatch == null) {
                        eosl = true;
                        if (leftbatches.isEmpty()) {
                            return outbatch;
                        }
                        break;
                    }
                    leftbatches.add(leftbatch);
                }

                /** Whenever a new left block came, we have to start the
                 ** scanning of right table
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("NestedJoin:error in reading the file");
                    System.exit(1);
                }
            }

            leftbatchsize = leftbatches.get(0).size();
            while(eosr == false) {
                try {
                    if (lpgcurs == 0 && lcurs == 0 && rcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }
                    for (i = lpgcurs; i < leftbatches.size(); ++i) {
                        for (l = lcurs; l <leftbatchsize; ++l) {
                            for (j = rcurs; j < rightbatch.size(); ++j) {
                                Tuple lefttuple = leftbatches.get(i).get(l);
                                Tuple righttuple = rightbatch.get(j);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        if (i == leftbatches.size() - 1 && l == leftbatchsize -1 && j == rightbatch.size() - 1) {  //case 1
                                            lpgcurs = 0;
                                            lcurs = 0;
                                            rcurs = 0;
                                        } else if (i != leftbatches.size() - 1 && l == leftbatchsize - 1 && j == rightbatch.size() - 1) {  //case 2
                                            lpgcurs = i + 1;
                                            lcurs = 0;
                                            rcurs = 0;
                                        } else if (i == leftbatches.size() - 1 && l != leftbatchsize - 1 && j == rightbatch.size() - 1) {  //case 3
                                            lpgcurs = i;
                                            lcurs = l + 1;
                                            rcurs = 0;
                                        } else if (i == leftbatches.size() - 1 && l == leftbatchsize - 1 && j != rightbatch.size() - 1){  //case 4
                                            lpgcurs = i;
                                            lcurs = l;
                                            rcurs = j + 1;
                                        } else if (i != leftbatches.size() - 1 && l != leftbatchsize - 1 && j == rightbatch.size() -  1) {  //case 5
                                            lpgcurs = i;
                                            lcurs = l + 1;
                                            rcurs = 0;
                                        } else if (i != leftbatches.size() - 1 && l == leftbatchsize - 1 && j != rightbatch.size() - 1) {  //case 6
                                            lpgcurs = i + 1;
                                            lcurs = l;
                                            rcurs = j + 1;
                                        } else if (i == leftbatches.size() - 1 && l != leftbatchsize - 1 && j != rightbatch.size() - 1) {  //case 7
                                            lpgcurs = i;
                                            lcurs = l;
                                            rcurs = j + 1;
                                        } else {  //case 8
                                            lpgcurs = i;
                                            lcurs = l;
                                            rcurs = j + 1;
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs= 0;
                        }
                        lcurs = 0;
                    }
                    lpgcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("NestedJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (IOException e) {
                    System.out.println("NestedJoin: Error in reading temporary file");
                    System.exit(1);
                } catch (ClassNotFoundException e) {
                    System.out.println("NestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                }
            }
        }
        return outbatch;

    }
    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }
}
