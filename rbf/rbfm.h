#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>

#include "../rbf/pfm.h"

using namespace std;

// Record ID
typedef struct
{
  unsigned pageNum;    // page number
  unsigned slotNum;    // slot number in the page
} RID;



// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;
typedef enum { INDEX = 0, RBF } convertType;
typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// = 
           LT_OP,      // <
           LE_OP,      // <=
           GT_OP,      // >
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;


/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
  RBFM_ScanIterator() {};
  ~RBFM_ScanIterator() {};

  // Never keep the results in the memory. When getNextRecord() is called, 
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);
  void reset();

  RC close();

  FileHandle fileHandle;
  vector<Attribute> recordDescriptor;
  string conditionAttribute;
  unsigned short conditionAttributeNum;
  map<string,int> attributeToNumMap;
  CompOp compOp;                  // comparision type such as "<" and "="
  void *value=NULL;       // used in the comparison
  vector<string> attributeNames; // a list of projected attributes
  int curPageNum=0,curSlotNum=0;
  unsigned char* scanNullIndicator=NULL;
  unsigned char* curPage=NULL;

};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC fileExists(const string &fileName);

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first, 
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);


  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

  // This method will be mainly used for debugging/testing. 
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

//helper functions
  bool compare(const AttrType attrType,
		  const unsigned char* data1 ,
		  const CompOp compOp,
		  const unsigned char* data2,
		  const unsigned short dataSize);

  //insert , read
  void convertData(const vector<Attribute> &recordDescriptor, const void *data,unsigned short& recordSize, unsigned char* myData, convertType ct);
  void convertData2(const vector<Attribute> &recordDescriptor, const void *myData, unsigned short& teacherDataSize,unsigned char* teacherData);
  void convertNthAttribute(unsigned char* myData,
  	unsigned char* nthAttributeData,
  	unsigned short& nthAttributeDataSize,
  	const unsigned short n,
  	const vector<Attribute> &recordDescriptor
  	);

  void convertScanData(
  		const vector<Attribute> &recordDescriptor,
  		const vector<string> &attributeNames,
  		map<string,int>& attributeToNumMap,
  		const void *myData,
  		unsigned short& scanDataSize,
  		unsigned char* scanData);


 //page
  short compactPage(unsigned char* curPage,short compactStartPos);
  unsigned short getStartOfEmptySpace(unsigned char* page);
  unsigned short emptySpaceCount(unsigned char* page);
  bool isTomb(unsigned char* page, unsigned slotNum);


  //slot

//debug tools
  void converDataToBits(void* data, int size);
  void printBitsOfOneByte(unsigned char c);

//get
  unsigned short getSlotCount(unsigned char* page);
  unsigned short getNthSlotStartPos(const unsigned char* curPage,int slotNum); //slotPos in slot directory
  unsigned short getSlotDirSpaceCount(unsigned char* curPage);

  short getRecordLength(const unsigned char* data, unsigned slotNum);
  short getRecordOffset(const unsigned char* page,unsigned slotNum);

  short getFirstTomb(unsigned char* page); //return -1 if no tomb exists in the slotDir
  short getFirstUnusedSlot(unsigned char* page);
  void getTombRID(unsigned char* page,unsigned slotNum,RID& tombRID);

  unsigned char getNthNullBit(unsigned short n,const void *data);
  unsigned char nthBitInByte(unsigned char* data,unsigned short n);
  int getActualByteForNullsIndicator(int numOfFields);

//update

  void updateEmptySpaceOffset(unsigned char* page,unsigned short newValue);
  void updateSlotOffset(unsigned char* curPage,unsigned short slotNum, short newOffset);
  void updateSlotCount(unsigned char* page,unsigned short newValue);
  void updateRecordLength(unsigned char* page, unsigned slotNum, short newVal);

//set
  void setNthNullBitToOne(void* data, int n );

public:

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
};
//known bugs
//update a record which is actually a tomb or unusedslot
//write a function to list all the slot from right to left for debugging purpose
#endif
