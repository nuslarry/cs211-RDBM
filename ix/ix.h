
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stdio.h>
#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

# define FREE_SPACE_OFFSET PAGE_SIZE-SHORT_BYTE
# define SLOT_COUNT_OFFSET PAGE_SIZE-SHORT_BYTE*2
# define NEXT_LEAF_PAGE_OFFSET PAGE_SIZE-SHORT_BYTE*2-INT_BYTE
# define RESERVE_OFFSET PAGE_SIZE-SHORT_BYTE*2-INT_BYTE-INT_BYTE*3
# define NULL_DESCRIPTOR_BYTE 1
# define DESCRIPTOR_SHORT_BYTE 2
# define POINTER_BYTE 2*5
# define KEY_OFFSET 2+1+2*5
typedef enum { Index , Leaf } indexPageType;

typedef enum { SUCCESS , FULL } returnType;

struct addReturnInfo{
	returnType rt;
	void* data;
	short dataSize;
	addReturnInfo(returnType rtVal, void* dataVal,short dataSizeVal){
		rt=rtVal;
		if(dataSizeVal!=0)
		    data=malloc(dataSizeVal);
		else
			data=NULL;
		dataSize=dataSizeVal;
		memcpy(data,dataVal,dataSizeVal);
	}
	addReturnInfo(const addReturnInfo& temp){
		rt=temp.rt;
		dataSize=temp.dataSize;
		data=malloc(dataSize);
    	memcpy(data,temp.data,dataSize);
    }
    ~addReturnInfo(){
    	free(data);
    }
};

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) ;


        addReturnInfo insertEntryToNthPage(IXFileHandle &ixfileHandle,
        		int n, const Attribute &attribute, const void *key, const RID &rid);
        addReturnInfo insertDataToPageWithSpace(void* page, const Attribute &attribute, void* data,short dataSize);

        addReturnInfo splitPage(int n, void* page, IXFileHandle &ixfileHandle,
        		const Attribute &attribute, void* data, short dataSize);

        RC deleteEntryInNthPage(IXFileHandle &ixfileHandle, int n, const Attribute &attribute, const void *key, const RID &rid);

        void deleteIthRecord(void* page, int i);

        void createLeafData(void* leafData, unsigned short& leafDataSize,
        		const Attribute &attribute, const void *key, const RID &rid);
        void createIndexData(void* indexData, unsigned short& indexDataSize,
        		const Attribute &attribute, const void *key, int leftChildren, int rightChildren);


        void initializeEmptyIndexPage(void* emptyHeaderPage);

        int findFirstPageWithNoIndex(IXFileHandle &ixfileHandle);
        int findInsertPageNum(void* page, const Attribute &attribute, const void* key);

        bool isKeySmaller(const Attribute &attribute, const void* key1, const void* key2);
        bool isKeyEqual(const Attribute & attribute, const void* key, const void* prevKey);

        vector<Attribute> getRecordDescriptor(const Attribute &attribute);
        short getIndexPageFreeSpace(void* indexPage);
        short getFreeSpaceOffset(void* page);
        short getSlotCount(void* indexPage) ;
        indexPageType getIndexPageType(void* curPage);
        short getIthRecordOffset(void* page, short i);
        short getIthRecordLen(void* page, short i);
        short getIthSlotOffset(short i);
        int getLeftChildren(const Attribute &attribute, void* indexRecord);
        int getRightChildren(const Attribute &attribute, void* indexRecord);
        int getridPageNum(const Attribute &attribute, void* indexRecord);
        int getridSlotNum(const Attribute &attribute, void* indexRecord);
        int getNextLeafPageNum(void* page);

        void extractKey(void* key, const Attribute &attribute, void* data);
        void printKey(const void* key, const Attribute &attribute);
        void updateFreeSpaceOffset(void* page, short val);
        void updateSlotCount(void* page, short val);
        void updateIthRecordLeftChild(const Attribute &attribute,void* page,int i,int newVal);
        void updateIthRecordRightChild(const Attribute &attribute,void* page,int i,int newVal);
        void updateIthSlotOffset(void* page,int i, short newVal);
        void updatedateNextLeafPage(void* page, int newVal);

        void copyKey(const Attribute & attribute, void* key1, void* key2) ;

        void printBtreeHelper(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum, string emptySpace);
        void printAllLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute);
        void printPageI(IXFileHandle &ixfileHandle, const Attribute &attribute,int i);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};


class IX_ScanIterator {
    public:
	    IXFileHandle* ixfileHandle;
        Attribute attribute;
        void* lowKey;
        void* highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;

        int curKeyPage=-1;
        int curKeySlotNum=-1;

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);
        void setFirstKeyHelper(int curPageNum);
        void setFirstKey();
        bool isKeyInRange(void* key);

        void reset();
        // Terminate index scan
        RC close();
    private:

};


class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
	RC openFile(const string &fileName);
	FileHandle& getMyFileHandle();
	int rootLeafPage();

	int getNumberOfPages();

    RC writePage(PageNum pageNum, void *data);
	RC readPage(PageNum pageNum, void *data);
	RC appendPage(const void *data);
	RC updateRootLeafpage(int newRootLeafPage);
	bool fileExists(const string &fileName);
	string curFileName;
    private:
	FileHandle myFileHandle;


};

#endif
