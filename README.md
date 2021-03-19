New functions implemented in the code
1. *Block Nested Loops join*
   
   Refer to class file src/qp/operators/BlockNestedJoin
   
   This class will perform Block Nested Loops Joins of two relations and return joined tuples
   
   The implementation of this class is quite similar to the implementation of NestedJoin class.
   The main difference between these two classes is that BlockNestedJoin maintains an arraylist of Batch which is
   a block of pages. And there are two cursors "lcurs" and "lpgcurs" for left relation; "lcurs" is for traversing tuples
   in one input buffer while "lpgcurs" is for traversing buffer pages.
   
2. *External Sort*

   The aim of this class is to sort a base operator. It is constructed using a base Operator, and generates a Sorted Run consisting of same data of base in ascending/descending order. 

   The idea is based on External Sort. It consists of Phase 1 and Phase 2 of external sort. 

   When next() is called, it will return the next Batch in final sorted run. 

3. *SortedRun*

   The basic idea of this class is to create a file consisting of Batch Objects, which simulates sorted runs in External Sort. 

   This class is a basic component for External Sort. All batches and all tuples insides these batches are sorted. When next() is called, the next Batch of the file will be fetched from the file and returned. 
   
4. *Sort Merge join*

   Refered to class file src/qp/operators/BlockNestedJoin. 
   
   This class will perform SortMergeJoin of two relationsand, and its next() returns a Batch consisting of joined tuples. 
   
   The basic idea of Sort Merge Join is to use 2 childern Operator to construct 2 ExternalSort instances. When next() is called, it pull data from finalSortedRuns in 2 ExternalSorts into the memory, and then merge them into an outbatch.

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

   Aggregate functions are realized in Project class by modified open() method and next() method.
   
   In the new open() method, the operator will determine whether tuples or aggregate statistics need to projected and
   the outcome will be stored in a new boolean variable called hasAggregation.
   
   Then, for the modified next() method, it will call next_aggregation() method to generate aggregate statistics if
   hasAggregation is true and will call next_noaggregation() method to project wanted tuples if hasAggregation is false.
   
   For next_aggregation() method, it will initiate an arraylist of Aggregation object. The Aggregation class is
   implemented to store aggregate statistics of one attribute in a relation and there are methods in Aggregation class
   that are used to update statistics and return statistics to the caller. For the arraylist of Aggregation object,
   each entry of the arraylist corresponds to one attribute in the projection list. After scanning the whole relation,
   the next_aggregation() will return an Batch object called outbatch which stores wanted aggregate data to the consumer.
   
   For next_noaggregation() method, the function remains unchanged as the original next() method but some details are
   modified to fully utilize a Batch object called outbatch.

8. *Project functions* 

   In addition to implementations mentioned in Point 7, the next_noaggregation method() is written to fully utilize
   outbatch. One int variable called cursor and one boolean variable called eof are added to facilitate traversing the 
   relation. cursor is used to traverse tuples in one input buffer while eof represents whether the end of the relation 
   is reached. The outbatch will be returned to the consumer only when the outbatch is full or the end of the base 
   relation is reached.
