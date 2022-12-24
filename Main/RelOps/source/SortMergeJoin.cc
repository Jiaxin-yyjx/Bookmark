
#ifndef SORTMERGE_CC
#define SORTMERGE_CC

#include "Aggregate.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include "Sorting.h"

SortMergeJoin ::SortMergeJoin(MyDB_TableReaderWriterPtr leftInputIn, MyDB_TableReaderWriterPtr rightInputIn,
                              MyDB_TableReaderWriterPtr outputIn, string finalSelectionPredicateIn,
                              vector<string> projectionsIn,
                              pair<string, string> equalityCheckIn, string leftSelectionPredicateIn,
                              string rightSelectionPredicateIn)
{
  leftInput = leftInputIn;
  rightInput = rightInputIn;
  output = outputIn;
  finalSelectionPredicate = finalSelectionPredicateIn;
  projections = projectionsIn;
  equalityCheck = equalityCheckIn;
  leftSelectionPredicate = leftSelectionPredicateIn;
  rightSelectionPredicate = rightSelectionPredicateIn;
}

void SortMergeJoin ::run()
{
  // Create the empty records.
  MyDB_RecordPtr leftInputRec1 = leftInput->getEmptyRecord();
  MyDB_RecordPtr rightInputRec1 = rightInput->getEmptyRecord();

  // Used for another enpty place to place the records for comperator.
  MyDB_RecordPtr leftInputRec2 = leftInput->getEmptyRecord();
  MyDB_RecordPtr rightInputRec2 = rightInput->getEmptyRecord();

  // Constructure all the comperator here.
  // The left comperator is used for checking tuples on left pages whether it is equals or not.
  function<bool()> leftComperator = buildRecordComparator(leftInputRec1, leftInputRec2, equalityCheck.first);
  function<bool()> rightComperator = buildRecordComparator(rightInputRec1, rightInputRec2, equalityCheck.second);
  function<bool()> leftComperator2 = buildRecordComparator(leftInputRec2, leftInputRec1, equalityCheck.first);

  // Citation: SUDO code reference structure from https://www.google.com/search?q=what+is+sort+merge+join+in+database&tbm=isch&source=iu&ictx=1&vet=1&fir=y4iQjTMgDCxlCM%252CiI8LVS_SBlOrWM%252C_&usg=AI4_-kTplMz6rIc2FmKoK_rNZinbZlPQ4Q&sa=X&ved=2ahUKEwj5j7mPlt_2AhUEa80KHcdDA3IQ9QF6BAgREAE&biw=1920&bih=1001&dpr=1#imgrc=y4iQjTMgDCxlCM&imgdii=ilP6cg5G_KuDzM
  /*Stage 1: Sorting*/
  int size = leftInput->getBufferMgr()->numPages / 2; // Citation: piazza: https://piazza.com/class/ky7jrq7vpbt1gr?cid=351
  MyDB_RecordIteratorAltPtr left = buildItertorOverSortedRuns(size, *leftInput, leftComperator, leftInputRec1, leftInputRec2, leftSelectionPredicate);
  MyDB_RecordIteratorAltPtr right = buildItertorOverSortedRuns(size, *rightInput, rightComperator, rightInputRec1, rightInputRec2, rightSelectionPredicate);

  // Citation: Copy from ScanJoin.cc
  // Get the schema that results from combining the left and right records
  MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema>();
  for (auto &p : leftInput->getTable()->getSchema()->getAtts())
  {
    mySchemaOut->appendAtt(p);
  }

  for (auto &p : rightInput->getTable()->getSchema()->getAtts())
  {
    mySchemaOut->appendAtt(p);
  }

  // Citation: Copy from ScanJoin.cc
  // Having  that schema on an page
  MyDB_RecordPtr combinedRec = make_shared<MyDB_Record>(mySchemaOut);
  combinedRec->buildFrom(leftInputRec1, rightInputRec1);

  //  now, get the final predicate over it
  func finalPredicate = combinedRec->compileComputation(finalSelectionPredicate);

  // and get the final set of computatoins that will be used to buld the output record
  vector<func> finalComputations;
  for (string s : projections)
  {
    finalComputations.push_back(combinedRec->compileComputation(s));
  }
  // Construct the compare compile computatipon
  func leftComputation = combinedRec->compileComputation(" < (" + equalityCheck.first + ", " + equalityCheck.second + ")");
  func rightComputation = combinedRec->compileComputation(" > (" + equalityCheck.first + ", " + equalityCheck.second + ")");
  func equalComputation = combinedRec->compileComputation(" == (" + equalityCheck.first + ", " + equalityCheck.second + ")");

  MyDB_RecordPtr outputRec = output->getEmptyRecord();
  MyDB_PageReaderWriter lastPage(true, *(leftInput->getBufferMgr()));

  /*Stage 2: Merging*/
  vector<MyDB_PageReaderWriter> pages;

  // If both of them have the records.
  if (left->advance() && right->advance())
  {
    bool flag = true;
    while (flag)
    {
      left->getCurrent(leftInputRec1);
      right->getCurrent(rightInputRec1);
      // if(r.A > q.B) then next tuple in q
      // if(r.A < q.B) then next tuple in r
      // if(r.A == q.B) then loop througth q and r
      if ((leftComputation()->toBool() && !left->advance()) || (rightComputation()->toBool() && !right->advance()))
      {

        break;
      }

      else if (equalComputation()->toBool())
      {
        lastPage.clear();
        pages.clear();
        pages.push_back(lastPage);
        lastPage.append(leftInputRec1);

        while (true)
        {
          // cout << "Get stuck at here3";
          if (!left->advance())
          {
            // If we don't have any record left in left.
            flag = false;
            break;
          }

          left->getCurrent(leftInputRec2);
          // While the it is still the equal truple in the same file
          if (!leftComperator() && !leftComperator2())
          {
            if (!lastPage.append(leftInputRec2))
            {
              MyDB_PageReaderWriter nextPage(true, *(leftInput->getBufferMgr()));
              lastPage = nextPage;
              pages.push_back(lastPage);
              lastPage.append(leftInputRec2);
            }
          }
          else
          {
            // If the next tuple is not the same as the previous one
            // Means that we the records are not the same, we leave the loop.
            break;
          }
        }

        while (true)
        {
          // cout << "Get stuck at here3";
          // Check whether the equal truple still hold in right page
          if (equalComputation()->toBool())
          {
            // Create a iter in the pages to check if the records are equals?
            MyDB_RecordIteratorAltPtr myIter;
            if (pages.size() != 1)
            {
              // If there area already some pages there
              myIter = getIteratorAlt(pages);
            }
            else
            {
              // If the first page is not full.
              myIter = pages[0].getIteratorAlt();
            }

            while (myIter->advance())
            {
              myIter->getCurrent(leftInputRec1);
              // Citation: copy from ScanJoin.cc
              if (finalPredicate()->toBool())
              {
                // run all of the computations
                int i = 0;
                for (auto &f : finalComputations)
                {
                  outputRec->getAtt(i++)->set(f());
                }
                // the record's content has changed because it
                // is now a composite of two records whose content
                // has changed via a read... we have to tell it this,
                // or else the record's internal buffer may cause it
                // to write old values
                outputRec->recordContentHasChanged();
                output->append(outputRec);
              }
            }
          }
          else
          {
            // If equal predicate is still hold.
            break;
          }
          // If no more tuple in the right page
          if (!right->advance())
          {
            flag = false;
            break;
          }
          right->getCurrent(rightInputRec1);
        }
      }
    }
  }
}
#endif
