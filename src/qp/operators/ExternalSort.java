package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

class ExternalSort extends Operator {
    Operator base;
    ArrayList<Integer> compareIndexes;
    String outfile;
    int filenum;
    int numBuff;
    int batchsize;
    ArrayList<Batch> mem;
    ArrayList<SortedRun> srs;
    SortedRun finalSR;

    public ExternalSort(int type, Operator base, ArrayList<Integer> compareIndexes, int numBuff) {
        super(type);
        this.base = base;
        this.compareIndexes = compareIndexes;
        this.schema = base.getSchema();
        this.numBuff = numBuff;
        mem = new ArrayList<>();
        for (int i = 0; i < numBuff; ++i) {
            mem.add(new Batch(batchsize));
        }
        filenum = 0;
        srs = new ArrayList<>();
    }

    @Override
    public boolean open() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if(!base.open()) return false;
        do {
            ArrayList<Tuple> tuplesInMem = new ArrayList<>();
            for (int i = 0; i < numBuff; ++i) {
                Batch curr = base.next();
                if (curr != null) mem.add(curr);
                else break;
                for (int j = 0; j < curr.size(); ++j)
                    tuplesInMem.add(curr.get(j));
            }
            Collections.sort(tuplesInMem, this::compareTuplesByIndex);
            srs.add(new SortedRun(tuplesInMem, batchsize));
        } while (base.next() != null);

        for (int pass = 0; ; pass++) {
            if (srs.size() == 1) {
                finalSR = srs.get(0);
                return true;
            }
            ArrayList<SortedRun> temp = mergeSortedRuns(srs, mem);
            srs = temp;
        }
    }

    @Override
    public Batch next() {
        return finalSR.next();
    }

    @Override
    public boolean close() {
        return super.close();
    }

    private int compareTuplesByIndex(Tuple left , Tuple right) {
        for (int cmpIdx : compareIndexes) {
            int cmpRes = Tuple.compareTuples(left, right, cmpIdx);
            if (cmpRes != 0) {
                return cmpRes;
            }
        }
        return 0;
    }
    private int findMaxTupleIndex(ArrayList<Batch> mem, int[] cursors, boolean[] isNull) {
        int maxIdx = -1; boolean foundNonNull = false;
        for (int i = 0; i < mem.size()-1; ++i) {
            if (isNull[i] || mem.get(i).size() == 0) continue;
            if (!foundNonNull) {
                maxIdx = i;
                foundNonNull = true;
            }
            if (compareTuplesByIndex(mem.get(i).get(cursors[i]), mem.get(maxIdx).get(cursors[maxIdx])) < 0) {
                maxIdx = i;
            }
        }
        return maxIdx;
    }

    private ArrayList<SortedRun> mergeSortedRuns(ArrayList<SortedRun> srs, ArrayList<Batch> mem) {
        ArrayList<SortedRun> newSrs = new ArrayList<>();
        for (int i = 0; i < (int) Math.ceil((double)srs.size() / (double)(numBuff-1)); ++i) {
            newSrs.add(new SortedRun(batchsize));
        }
        for (int i = 0; i < srs.size(); i += (numBuff-1)) {
            SortedRun outSr = new SortedRun(batchsize);
            mem = new ArrayList<>();
            for (int j = 0; j < numBuff-1; ++j) {
                if (i+j >= srs.size()) {
                    break;
                }
                Batch temp = srs.get(i+j).next();
                if (temp != null || temp.size() > 0)
                    mem.add(temp);
                else
                    break;
            }
            int[] cursors = new int[mem.size()];
            Arrays.fill(cursors, 0);
            boolean[] isNull = new boolean[mem.size()];
            Arrays.fill(isNull, false);
            mem.add(new Batch(batchsize));
            Batch outbatch = mem.get(mem.size()-1);
            while (true) {
                int maxIdx = findMaxTupleIndex(mem, cursors, isNull);
                if (maxIdx == -1) {
                    // all runs are used
                    outSr.add(outbatch);
                    break;
                }
                outbatch.add(mem.get(maxIdx).get(cursors[maxIdx]));
                if (outbatch.isFull())
                    outSr.add(outbatch);
                cursors[maxIdx]++;
                if (cursors[maxIdx] == mem.get(maxIdx).size()) {
                    Batch temp = srs.get(i+maxIdx).next();
                    if (temp != null) {
                        mem.set(maxIdx, temp);
                        cursors[maxIdx] = 0;
                    }
                    else isNull[maxIdx] = true;
                }
            }
            newSrs.set(i/(numBuff-1), outSr);
        }
        return newSrs;
    }
}
