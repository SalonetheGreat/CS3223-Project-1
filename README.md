New functions implemented in the code
1. *Block Nested Loops join*

2. *External Sort*

3. *SortedRun*
   
4. *Sort Merge join*

5. *Distinct*
   
   see src/qp/operators/Distinct.java
   
   This class provides a distinct operation and returns distinct tuples.
   
   open() will generate an external sort operator from the base to sort the given base table.
   
   next() will derive the distinct tuples from the external sort and output will be distinct tuples.

6. *Orderby*
   
   see src/qp/oprators/Orderby.java
   
   This class provides an orderby operation and returns tuples ordered by given attributes in Asc/Des order (depending on the input).
   
   open() will generate an external sort operator from the base to sort the given base table in Asc/Dec order based on given attributes.
   
   next() will output the sorted tuples in external sort.

7. *Aggregate functions (MIN, MAX, COUNT, AVG)*
