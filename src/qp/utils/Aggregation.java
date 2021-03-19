package qp.utils;

public class Aggregation {

    int type;      //type of the attribute
    int COUNT;
    int MIN_INT;
    int MAX_INT;
    double SUM;
    float MIN_FLT ;
    float MAX_FLT;
    //float SUM_FLT;
    String MIN_STR = null;
    String MAX_STR = null;


    public Aggregation(Object data, int type) {
        this.type = type;
        //System.out.println("this.type: " + this.type);
        if (data == null) {
            COUNT = 0;
        } else {
            COUNT += 1;
            initialize(data);
        }
    }
    public void initialize (Object data) {
        //System.out.println("this.type: " + this.type);
        if (this.type == Attribute.REAL) {
            float num = ((Float)data).floatValue();
            MIN_FLT = num;
            MAX_FLT = num;
            SUM += num;
        } else if (this.type == Attribute.INT) {
            int num = ((Integer)data).intValue();
            //System.out.println(num);
            MAX_INT = num;
            MIN_INT = num;
            SUM += num;
        } else {
            MAX_STR = (String) data;
            MIN_STR = (String) data;
        }
    }
    public void update (Object data) {
        if (data == null) {
            return;
        } else {
            if (COUNT == 0) {
                initialize(data);
                COUNT += 1;
                return;
            }
            COUNT += 1;
            if (type == Attribute.INT) {
                int num = ((Integer)data).intValue();
                //System.out.println(num);
                SUM += num;
                if (MAX_INT < num) {
                    MAX_INT = num;
                }
                if (MIN_INT > num) {
                    MIN_INT = num;
                }
            } else if (type == Attribute.REAL){
                float num =((Float)data).floatValue();
                SUM += num;
                if (MAX_FLT < num) {
                    MAX_FLT = num;
                }
                if (MIN_FLT > num) {
                    MIN_FLT = num;
                }
            } else {
                String str = (String) data;
                if (MAX_STR.compareTo(str) < 0) {
                    MAX_STR = str;
                }
                if (MIN_STR.compareTo(str) > 0) {
                    MIN_STR = str;
                }
            }
        }

    }

    public Integer returnCount() {
        return COUNT;
    }

    public String returnMaxStr() {
        return MAX_STR;
    }

    public String returnMinStr() {
        return MIN_STR;
    }

    public Integer returnMaxInt() {
        if (COUNT == 0) return null;
        else return MAX_INT;
    }

    public Integer returnMinInt() {
        if (COUNT == 0) return null;
        else return MIN_INT;
    }

    public Float returnMaxFlt() {
        if (COUNT == 0) return null;
        else return MAX_FLT;
    }

    public Float returnMinFlt() {
        if (COUNT == 0) return null;
        else return MIN_FLT;
    }

    public Float returnAvg() {
        if (COUNT == 0) {
            return null;
        } else {
            return Float.valueOf((float) (SUM/COUNT));
        }
    }
}
