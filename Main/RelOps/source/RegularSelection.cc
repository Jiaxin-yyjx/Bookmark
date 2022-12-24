
#ifndef REG_SELECTION_C
#define REG_SELECTION_C

#include "RegularSelection.h"

RegularSelection ::RegularSelection(MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
                                    string selectionPredicateIn, vector<string> projectionsIn)
{
  // Put everything into the variables
  input = inputIn;
  output = outputIn;
  selectionPredicate = selectionPredicateIn;
  projections = projectionsIn;
}

void RegularSelection ::run()
{
  // Create two empty record
  MyDB_RecordPtr inputRec = input->getEmptyRecord();
  MyDB_RecordPtr outputRec = output->getEmptyRecord();

  // Generate the function that equivalent to the input string SQL
  // Citation: from ScanJoin.cc
  func finalPredicate = inputRec->compileComputation(selectionPredicate);

  // Citation: from ScanJoin.cc
  vector<func> finalComputations;
  for (string s : projections)
  {
    finalComputations.push_back(inputRec->compileComputation(s));
  }
  // cout << "Here";
  MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt();
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
