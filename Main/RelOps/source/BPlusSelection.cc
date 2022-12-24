
#ifndef BPLUS_SELECTION_C
#define BPLUS_SELECTION_C

#include "BPlusSelection.h"

BPlusSelection ::BPlusSelection(MyDB_BPlusTreeReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
                                MyDB_AttValPtr lowIn, MyDB_AttValPtr highIn,
                                string selectionPredicateIn, vector<string> projectionsIn)
{
  input = inputIn;
  output = outputIn;
  low = lowIn;
  high = highIn;
  selectionPredicate = selectionPredicateIn;
  projections = projectionsIn;
}

void BPlusSelection ::run()
{
  // Create two empty record
  MyDB_RecordPtr inputRec = input->getEmptyRecord();
  MyDB_RecordPtr outputRec = output->getEmptyRecord();

  // now get the predicate
  // Citation: from ScanJoin.cc
  func finalPredicate = inputRec->compileComputation(selectionPredicate);

  // Citation: from ScanJoin.cc
  vector<func> finalComputations;
  for (string s : projections)
  {
    finalComputations.push_back(inputRec->compileComputation(s));
  }

  // TODO: Get call the BPlus tree getRange methods.
  MyDB_RecordIteratorAltPtr myIter = input->getRangeIteratorAlt(low, high);
  while (myIter->advance())
  {
    myIter->getCurrent(inputRec);
    // Citation: from ScanJoin.cc
    // check to see if it is accepted by the join predicate
    // if this record is selected by select
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

#endif
