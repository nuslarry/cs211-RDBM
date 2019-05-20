#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    if(fileExists(fileName)){
    	cout<<"already exist";
    	return -1;
    }else{
        ofstream myOfs(fileName);
    	if(myOfs.bad()){
    		return -1;
    	}else {
    		unsigned char* headerPage=(unsigned char*)malloc(PAGE_SIZE);
    		initializeHeaderPage(headerPage);
    		ofstream fs(fileName, ofstream::binary);
    		fs.seekp(0, fs.beg);
    		fs.write((char*)headerPage,PAGE_SIZE);
    		free(headerPage);
    		return 0;
    	}
    }
}
bool PagedFileManager::fileExists(const string &fileName){
	ifstream myIfs(fileName);
	return myIfs.good();
}
void PagedFileManager::initializeHeaderPage(unsigned char* headerPage){
	unsigned zero= 0;
	for(int i=0;i<4;i++){
		memcpy(headerPage+UNSIGNED_BYTE*i, &zero, UNSIGNED_BYTE);
	}
	int negativeOne=-1;
	memcpy(headerPage+UNSIGNED_BYTE*4, &negativeOne,INT_BYTE);
	return;
}
RC PagedFileManager::destroyFile(const string &fileName)
{
    if(fileExists(fileName)){
    	if(remove(fileName.c_str())!=0) return -1;
    	else return 0;
    }else{
        return -1;
    }
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if(!fileExists(fileName))return -1;
    if(fileHandle.inUse()){
        return -1;
    }else{
        return fileHandle.openFile(fileName);
    }
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	if(fileHandle.inUse()){
		return fileHandle.closeFile();
	}else return -1;

}


FileHandle::FileHandle()
{
	curPageNum =-2; // -1 is headerPage(user cannot see it)
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}
RC FileHandle::readPage(PageNum pageNum, void *data){
	return readPage(pageNum,data,User);
}

RC FileHandle::readPage(PageNum pageNum, void *data,Permission p)
{

	int curNumOfPages=getNumberOfPages();
	if(pageNum>=curNumOfPages && pageNum!=-1)return -1;
	curFile.seekp(PAGE_SIZE*(pageNum+1), curFile.beg);
	curFile.read((char*)data,PAGE_SIZE);
    if(curFile.good()){
    	if(p==User)readPageCounter++;
    	return 0;
    }
    else return -1;
}

RC FileHandle::writePage(PageNum pageNum,const void *data){
	return writePage(pageNum,data,User);
}

RC FileHandle::writePage(PageNum pageNum, const void *data,Permission p)
{
	int curNumOfPages=getNumberOfPages();
	if(pageNum>=curNumOfPages && pageNum!=-1)return -1;
	curFile.seekp(PAGE_SIZE*(pageNum+1), curFile.beg);
	if(curFile.fail())return -1;
	curFile.write((char*)data,PAGE_SIZE);
	if(curFile.fail())return -1;
	else {
		if(p==User)writePageCounter++;
		return 0;
	}
}

RC FileHandle::appendPage(const void *data){
	return appendPage(data,User);
}

RC FileHandle::appendPage(const void *data,Permission p)
{
	int curNumOfPages=getNumberOfPages();
	curFile.seekp(PAGE_SIZE*(curNumOfPages+1), curFile.beg);
	if(curFile.fail())return -1;
	curFile.write((char*)data,PAGE_SIZE);
	if(curFile.fail())return -1;
	else {

		if(p==User)appendPageCounter++;
//cout<<"curPageNum="<<curPageNum<<endl;
		curPageNum++;
//		cout<<"curPageNum="<<curPageNum<<endl;
		return 0;
	}
}
unsigned char* FileHandle::createEmptyPage(){
	unsigned char* emptyHeaderPage=(unsigned char*)malloc(PAGE_SIZE);
    initializeEmptyPage(emptyHeaderPage);
    return emptyHeaderPage;
}

void FileHandle::initializeEmptyPage(unsigned char* emptyHeaderPage){
	emptyHeaderPage[PAGE_SIZE-1]=0;
	short freeSpaceOffset=0;
	memcpy(&emptyHeaderPage[PAGE_SIZE-sizeof(short)],&freeSpaceOffset,sizeof(short));
	memcpy(&emptyHeaderPage[PAGE_SIZE-2*sizeof(short)],&freeSpaceOffset,sizeof(short));
}

unsigned short FileHandle::getAvailSpace(PageNum pageNum){
	unsigned char* page=(unsigned char*)malloc(PAGE_SIZE);
	readPage(pageNum,page);
	unsigned short emptySpaceOffset,slotNum;
	memcpy(&emptySpaceOffset,&page[PAGE_SIZE]-sizeof(short),sizeof(short));
	memcpy(&slotNum,&page[PAGE_SIZE]-sizeof(short)*2,sizeof(short));
	free(page);
	return PAGE_SIZE-emptySpaceOffset-slotNum*SLOT_BYTE-sizeof(short)*2;
}

unsigned FileHandle::getNumberOfPages()
{
	return curPageNum;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	unsigned char * headerPage=(unsigned char*)malloc(PAGE_SIZE);
	readPage(-1,headerPage,SystemManager);
	memcpy(&readPageCount,&readPageCounter,UNSIGNED_BYTE);
	memcpy(&writePageCount,&writePageCounter,UNSIGNED_BYTE);
	memcpy(&appendPageCount,&appendPageCounter,UNSIGNED_BYTE);
	writePage(-1,headerPage,SystemManager);
	free(headerPage);
    return 0;
}
bool FileHandle::inUse(){return curFile.is_open();}

RC FileHandle::openFile(const string &fileName){
	curFile.open(fileName);
    if(curFile.good()){
    	curFileName=fileName;
    	unsigned char* headerPage=(unsigned char*)malloc(PAGE_SIZE);
    	curFile.read((char*)headerPage,PAGE_SIZE);
        memcpy(&curPageNum,headerPage,INT_BYTE);
        memcpy(&readPageCounter,headerPage+INT_BYTE,UNSIGNED_BYTE);
        memcpy(&writePageCounter,headerPage+INT_BYTE*2,UNSIGNED_BYTE);
        memcpy(&appendPageCounter,headerPage+INT_BYTE*3,UNSIGNED_BYTE);
        memcpy(&rootLeafPage,headerPage+INT_BYTE*4,INT_BYTE);
        free(headerPage);
    	return 0;
    }
    else return -1;
}

RC FileHandle::closeFile(){
	unsigned char* headerPage=(unsigned char*)malloc(PAGE_SIZE);
	readPage(-1,headerPage,SystemManager);
    memcpy(headerPage,&curPageNum,INT_BYTE);
    memcpy(headerPage+INT_BYTE,&readPageCounter,UNSIGNED_BYTE);
    memcpy(headerPage+INT_BYTE*2,&writePageCounter,UNSIGNED_BYTE);
    memcpy(headerPage+INT_BYTE*3,&appendPageCounter,UNSIGNED_BYTE);
    memcpy(headerPage+INT_BYTE*4,&rootLeafPage,INT_BYTE);
	writePage(-1,headerPage,SystemManager);
	free(headerPage);
	curFile.close();
    if(curFile.good())return 0;
    else return -1;
}


