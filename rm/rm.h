
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"
using namespace std;

# define RM_EOF (-1)  // end of a scan operator


class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  void reset();
  RC close();

  RBFM_ScanIterator rbfm_ScanIterator;
};


// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() {};  	// Constructor
  ~RM_IndexScanIterator() {}; 	// Destructor


  IX_ScanIterator ix_ScanIterator;
  IXFileHandle ixFileHandle;


  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
  void reset(){
	  ix_ScanIterator.reset();
  };
  RC close();            			// Terminate index scan
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);
  RC deleteTable(const string &tableName, Permission p);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);
  RC insertTuple(const string &tableName, const void *data, RID &rid, Permission p);

  RC deleteTuple(const string &tableName, const RID &rid);
  RC deleteTuple(const string &tableName, const RID &rid, Permission p);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);
  RC updateTuple(const string &tableName, const void *data, const RID &rid, Permission p);
  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);



//helper functions

bool tableExists(const string &tableName);

void prepareScanValue(void* returnValue,AttrType attrType,void* data,int dataSize);

void createTableRecordDescriptor(vector<Attribute> &recordDescriptor);
void createColumnRecordDescriptor(vector<Attribute> &recordDescriptor);
void prepareTableTuple(
		int fieldCount,
		unsigned char *nullFieldsIndicator,
		const int tableId,
		const string &tableName,
		const string &fileName,
		const int flag,
		void *tuple,
		unsigned short& tupleSize
		);
void prepareColumnTuple(
		int fieldCount,
		unsigned char *nullFieldsIndicator,
		const int tableId,
		const string &columnName,
		const int columnType,
		const int columnLength,
		const int columnPosition,
		void *tuple,
		unsigned short& tupleSize
		);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);

  Attribute getAttributeGivenName(const vector<Attribute> attrs, const string &attributeName);


  void convertNthAttributeTeacherData(vector<Attribute>& attrs, int n,const void* teacherData,
		  void* nthAttributeTeacherData,unsigned short& nthAttributeTeacherDataSize);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);

private:
  vector<Attribute> tableRecordDescriptor;
  vector<Attribute> columnRecordDescriptor;


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
};

#endif
