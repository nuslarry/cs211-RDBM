#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#define CHAR_BYTE 1
#define SHORT_BYTE 2
#define INT_BYTE 4
#define FLOAT_BYTE 4
#define SLOT_BYTE 4
#define UNSIGNED_BYTE 4
#define TOMB_BYTE 4
#include <string>
#include <climits>
#include <fstream>
#include <iostream>
#include <math.h>
#include <stdio.h>
#include <cstring>
#include <assert.h>
#include <map>
#include <algorithm>

typedef enum { SystemManager , User } Permission;

using namespace std;

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                                  // Access to the _pf_manager instance

    RC createFile    (const string &fileName);                            // Create a new file
    bool fileExists(const string &fileName);
    void initializeHeaderPage(unsigned char* headerPage);
    RC destroyFile   (const string &fileName);                            // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle);    // Open a file
    RC closeFile     (FileHandle &fileHandle);                            // Close a file

protected:
    PagedFileManager();                                                   // Constructor
    ~PagedFileManager();                                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    // variables to keep the counter for each operation
	int curPageNum;
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    int rootLeafPage;
    
    FileHandle();                                                         // Default constructor
    ~FileHandle();                                                        // Destructor

    RC readPage(PageNum pageNum, void *data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);
    unsigned char* createEmptyPage();// Append a specific page
    void initializeEmptyPage(unsigned char* emptyHeaderPage);
    unsigned short getAvailSpace(PageNum pageNum);
    unsigned getNumberOfPages();                                          // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables
    bool inUse();
    RC openFile(const string &fileName);
    RC closeFile();

    //debug tools
    RC readPage(PageNum pageNum, void *data,Permission p);
    RC writePage(PageNum pageNum, const void *data,Permission p);
    RC appendPage(const void *data,Permission p);
    string curFileName;
private:
    fstream curFile;

}; 

#endif
