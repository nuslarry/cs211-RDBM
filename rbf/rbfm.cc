#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::fileExists(const string &fileName){
	return PagedFileManager::instance()->fileExists(fileName);
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	return PagedFileManager::instance()->openFile(fileName,fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	unsigned short recordSize;
	//conver data to our own form and calculate the size of it and store it in recordSize
	unsigned char* myData=(unsigned char*)malloc(PAGE_SIZE);
	convertData(recordDescriptor,data,recordSize,myData, RBF); //free it later

    //find the first available page to insert the record, if this page is not found, append a new page
	unsigned curPageNum=fileHandle.getNumberOfPages();
    short numOfirstAvailPage=-1;
    for(unsigned i=0;i<curPageNum;i++){
    	unsigned short availSpace=fileHandle.getAvailSpace(i);
    	if(availSpace>=recordSize+SHORT_BYTE*2){  //SHORT_BYTE*2 is reserved for the new element in slot dir
    		numOfirstAvailPage=i;
    		break;
    	}

    }

    if(numOfirstAvailPage<0){

    	unsigned char* emptyPage=fileHandle.createEmptyPage();
    	fileHandle.appendPage(emptyPage);
    	numOfirstAvailPage=fileHandle.getNumberOfPages()-1;
    	free(emptyPage);
    }


    //insert the record to the page
    unsigned char* firstAvailPage=(unsigned char*)malloc(PAGE_SIZE);
    fileHandle.readPage(numOfirstAvailPage,firstAvailPage);
    unsigned short startOfEmptySpace=getStartOfEmptySpace(firstAvailPage);
    unsigned short slotCount=getSlotCount(firstAvailPage);
    memcpy(firstAvailPage+startOfEmptySpace,myData,recordSize);



    //if unusedSlot exists
    short firstUnusedSlot=getFirstUnusedSlot(firstAvailPage);
    if(firstUnusedSlot!=-1){
    	unsigned short nthSlotStartPos=getNthSlotStartPos(firstAvailPage,firstUnusedSlot);
    	memcpy(firstAvailPage+nthSlotStartPos,&startOfEmptySpace,SHORT_BYTE);
    	memcpy(firstAvailPage+nthSlotStartPos+SHORT_BYTE,&recordSize,SHORT_BYTE);
    	startOfEmptySpace+=recordSize;
        updateEmptySpaceOffset(firstAvailPage,startOfEmptySpace);
        //no need to nupdate slotcount
        rid.pageNum=numOfirstAvailPage;
        rid.slotNum=firstUnusedSlot;
        fileHandle.writePage(numOfirstAvailPage,firstAvailPage);
        free(firstAvailPage);
        free(myData);
        return 0;
    }
    //


    unsigned short posOfNextSlotEle=PAGE_SIZE-SHORT_BYTE*2-SLOT_BYTE*(slotCount+1);
    memcpy(firstAvailPage+posOfNextSlotEle,&startOfEmptySpace,SHORT_BYTE);
    memcpy(firstAvailPage+posOfNextSlotEle+SHORT_BYTE,&recordSize,SHORT_BYTE);
    startOfEmptySpace+=recordSize;
    updateEmptySpaceOffset(firstAvailPage,startOfEmptySpace);
    updateSlotCount(firstAvailPage,slotCount+1);
    rid.pageNum=numOfirstAvailPage;
    rid.slotNum=slotCount;
    fileHandle.writePage(numOfirstAvailPage,firstAvailPage);
    free(firstAvailPage);
    free(myData);
    return 0;
}




void RecordBasedFileManager::convertData(const vector<Attribute> &recordDescriptor, const void *data,unsigned short& recordSize, unsigned char* myData, convertType ct){
    unsigned short numOfFields=recordDescriptor.size();
    unsigned short nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(numOfFields);
    void* dataInRecord=(unsigned char*)malloc(PAGE_SIZE);
    unsigned short curOffset=nullFieldsIndicatorActualSize;// offset of input data
    unsigned short curOffsetFromStartOfDataInRecord=0;
    //calcuate the offsets from the start of start to the end of the  the fields
    vector<unsigned short> offsetFromStartOfDataInRecord;
	for(unsigned short i=0;i<numOfFields;i++){

		Attribute curAttribute=recordDescriptor[i];
//		cout<<"i="<<i<<",type="<<curAttribute.type<<endl;
        unsigned char nullBit=getNthNullBit(i,data);
        if(nullBit==1){
//        	cout<<i<<"is null"<<endl;
        	offsetFromStartOfDataInRecord.push_back(curOffsetFromStartOfDataInRecord);
        	continue;
        }

		if(curAttribute.type==TypeVarChar){
			int varCharLen; //since the unit of the length stored in data is int
			memcpy(&varCharLen,(unsigned char*)data+curOffset,sizeof(int));
			memcpy((char*)dataInRecord+curOffsetFromStartOfDataInRecord,&varCharLen,INT_BYTE);
			curOffset+=sizeof(int);
			if(ct==INDEX)curOffsetFromStartOfDataInRecord+=INT_BYTE;
//			cout<<"varCharLen="<<varCharLen<<endl;
//		    	cout<<"curOffset"<<curOffset<<endl;

			memcpy((char*)dataInRecord+curOffsetFromStartOfDataInRecord,(unsigned char*)data+curOffset,varCharLen);
//		        string test66("ssda");
//		        cout<<"test66="<<test66<<endl;
			curOffset+=varCharLen;
			curOffsetFromStartOfDataInRecord+=varCharLen;
			offsetFromStartOfDataInRecord.push_back(curOffsetFromStartOfDataInRecord);
		}else{

			memcpy((char*)dataInRecord+curOffsetFromStartOfDataInRecord,(unsigned char*)data+curOffset,sizeof(int));
			int test;
			memcpy(&test,(unsigned char*)data+curOffset,INT_BYTE);

//			cout<<"curOffsetFromStartOfDataInRecord="<<curOffsetFromStartOfDataInRecord<<endl;

			curOffset+=sizeof(int);
			curOffsetFromStartOfDataInRecord+=sizeof(int);
			offsetFromStartOfDataInRecord.push_back(curOffsetFromStartOfDataInRecord);

		}
	}

	//create  the real record that is going to be returned
    recordSize=sizeof(short)+nullFieldsIndicatorActualSize+sizeof(short)*numOfFields+curOffsetFromStartOfDataInRecord;
    unsigned short ansOffset=0;

    memcpy(myData+ansOffset,&numOfFields,SHORT_BYTE);
    ansOffset+=SHORT_BYTE;

    memcpy(myData+ansOffset,data,nullFieldsIndicatorActualSize);
    ansOffset+=nullFieldsIndicatorActualSize;
    for(int i=0;i<numOfFields;i++){
//    	unsigned short recordOffset=SHORT_BYTE*(numOfFields-i)+offsetFromStartOfDataInRecord[i];
    	memcpy(myData+ansOffset,&offsetFromStartOfDataInRecord[i],SHORT_BYTE);
    	ansOffset+=SHORT_BYTE;

    }

    memcpy(myData+ansOffset,dataInRecord,curOffsetFromStartOfDataInRecord);
    ansOffset+=curOffsetFromStartOfDataInRecord;
    recordSize=ansOffset;
    free(dataInRecord);


}
unsigned char RecordBasedFileManager::getNthNullBit(unsigned short n,const void *data){
	return nthBitInByte((unsigned char*)data+n/CHAR_BIT,n%CHAR_BIT);
}


unsigned char RecordBasedFileManager::nthBitInByte(unsigned char* data,unsigned short n){
    char c=data[0];
    if(c & 1<<(7-n))return 1;
    else return 0;
}

void RecordBasedFileManager::setNthNullBitToOne(void* data, int n ){
	  unsigned char* start=(unsigned char*)data;
	  int bytePos=n/8;
	  int bitPos=n%8;
	  start=start+bytePos;
	  start[0]=start[0] | 1<<(7-bitPos);
	  return;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getActualByteForNullsIndicator(int numOfFields) {
    return ceil((double) numOfFields / CHAR_BIT);
}
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	unsigned char* page=(unsigned char*) malloc(PAGE_SIZE);
    RC readResult=fileHandle.readPage(rid.pageNum,page);
    assert(readResult==0 && "read Page Fail");
    short ridOffset=getRecordOffset(page,rid.slotNum),ridLen=getRecordLength(page,rid.slotNum);
    if(ridOffset==-1)return -1;
    if(ridLen==-1){
    	RID tombRID;
    	getTombRID(page,rid.slotNum,tombRID);
    	return readRecord(fileHandle,recordDescriptor,tombRID,data);
    }
    unsigned char* myData=(unsigned char*)malloc(ridLen);
    memcpy(myData,page+ridOffset,ridLen);
    unsigned short techerDataSize;

    unsigned char* teacherData=(unsigned char*)malloc(PAGE_SIZE);

    convertData2(recordDescriptor,myData,techerDataSize,teacherData);

    memcpy(data,teacherData,techerDataSize);

    free(myData);

    free(teacherData);
    free(page);

	return 0;
}
void RecordBasedFileManager::convertData2(const vector<Attribute> &recordDescriptor, const void *myData, unsigned short& teacherDataSize,unsigned char* teacherData){
	teacherDataSize=0;
    unsigned short fieldCount=recordDescriptor.size();
    unsigned actualByteForNullsIndicator=getActualByteForNullsIndicator(fieldCount);
    teacherDataSize=0;

    memcpy(teacherData+teacherDataSize,(unsigned char*)myData+SHORT_BYTE,actualByteForNullsIndicator);
    teacherDataSize+=actualByteForNullsIndicator;

    for(int i=0;i<fieldCount;i++){

    	const unsigned char* temp=(unsigned char*)myData;
        if(getNthNullBit(i,temp+SHORT_BYTE)==1)continue;
    	Attribute a=recordDescriptor[i];
    	unsigned short startOffset,endOffset;
    	unsigned short inRecordOffset;
    	if(i==0)startOffset=SHORT_BYTE+actualByteForNullsIndicator+SHORT_BYTE*fieldCount;
    	else {
    		memcpy(&inRecordOffset,(unsigned char*)myData+SHORT_BYTE+actualByteForNullsIndicator+SHORT_BYTE*(i-1),SHORT_BYTE);
    		startOffset=SHORT_BYTE+actualByteForNullsIndicator+SHORT_BYTE*fieldCount+inRecordOffset;
    	}
    	memcpy(&inRecordOffset,(unsigned char*)myData+SHORT_BYTE+actualByteForNullsIndicator+SHORT_BYTE*(i),SHORT_BYTE);
    	endOffset=SHORT_BYTE+actualByteForNullsIndicator+SHORT_BYTE*fieldCount+inRecordOffset;

    	int len=endOffset-startOffset;

    	if(a.type==TypeVarChar){
            memcpy(teacherData+teacherDataSize,&len,INT_BYTE);
            teacherDataSize+=INT_BYTE;
    	}
    	memcpy(teacherData+teacherDataSize,(unsigned char*)myData+startOffset,len);
    	teacherDataSize+=len;

    }

}


RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	unsigned char nullDiscriptor;
	memcpy(&nullDiscriptor,data,1);

	unsigned short fieldCount=recordDescriptor.size();
	unsigned short dataOffset=getActualByteForNullsIndicator(fieldCount);

	for(int i=0;i<fieldCount;i++){
		Attribute a=recordDescriptor[i];

		unsigned char nthNullBit=getNthNullBit(i,data);
//		if(nthNullBit==1)cout<<"1";
//		else cout<<"0";


		if(nthNullBit==0){
			if(a.type==TypeVarChar){
				unsigned len;
				memcpy(&len,(unsigned char*)data+dataOffset,INT_BYTE);
				dataOffset+=INT_BYTE;
                unsigned char* varChar=(unsigned char*)malloc(1000);
                memcpy(varChar,(unsigned char*)data+dataOffset,len);
                dataOffset+=len;
                string name;
                for(int j=0;j<len;j++)name+=varChar[j];
                cout<<a.name<<":"<< name<<endl;
                free(varChar);

			}else if(a.type==TypeReal){
                float f;
                memcpy(&f,(unsigned char*)data+dataOffset,sizeof(float));
                dataOffset+=sizeof(float);
                cout<<a.name<<": "<<f<<endl;
			}else if(a.type==TypeInt){
                int number;
                memcpy(&number,(unsigned char*)data+dataOffset,sizeof(int));
                dataOffset+=sizeof(int);
                cout<<a.name<<": "<<number<<endl;
			}
		}else{
				cout<<a.name<<":"<< "null"<<endl;

		}
	}
    cout<<"----------------------"<<endl;
    return -1;
}
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){

	unsigned short recordSize;
	unsigned char* myData=(unsigned char*)malloc(PAGE_SIZE);

	convertData(recordDescriptor,data,recordSize, myData, RBF);

	unsigned char* curPage=(unsigned char*)malloc(PAGE_SIZE);

	fileHandle.readPage(rid.pageNum,curPage,SystemManager);
//	cout<<"origin: slotnum in the page="<<getSlotCount(curPage)<<endl;
    short curRecordLength=getRecordLength(curPage,rid.slotNum);
    short curRecordOffset=getRecordOffset(curPage,rid.slotNum);
    if(curRecordOffset==-1)return -1;

    if(curRecordLength==-1){
    	RID tomb;
    	getTombRID(curPage,rid.slotNum,tomb);
    	updateRecord(fileHandle,recordDescriptor,data,tomb);
        free(myData);
        free(curPage);
    	return 0;

    }
    short recordOffset=getRecordOffset(curPage,rid.slotNum);
    assert(recordOffset<=PAGE_SIZE && "recordOffset should be smaller than 4096");
    if(recordSize==curRecordLength){

        memcpy(curPage+recordOffset,myData,recordSize);
    }else if(recordSize<curRecordLength){

    	memcpy(curPage+recordOffset,myData,recordSize);
    	updateRecordLength(curPage,rid.slotNum,recordSize);

        unsigned short compactEndOffset=compactPage(curPage,recordOffset+recordSize);

        updateEmptySpaceOffset(curPage,compactEndOffset);
    }else{//recordSize>curRecordLength

        if(emptySpaceCount(curPage)+curRecordLength>=recordSize){

        	unsigned short compactEndOffset=compactPage(curPage,recordOffset);


        	unsigned char* temp=(unsigned char*)malloc(recordSize);
        	memcpy(temp,myData,recordSize);
        	memcpy(curPage+compactEndOffset,temp,recordSize);
        	updateSlotOffset(curPage,rid.slotNum,compactEndOffset);
        	updateRecordLength(curPage,rid.slotNum,recordSize);
        	updateEmptySpaceOffset(curPage,compactEndOffset+recordSize);
        	free(temp);
        }else{//create tomb, insert the record to a new page
        	//insert the record to a new page and get a new RID

        	RID newRid;
        	insertRecord(fileHandle, recordDescriptor, data, newRid);

        	//insert tomb based on the new RID
        	short compactEndOffset=compactPage(curPage,recordOffset);
        	unsigned short newRIDpageNum=newRid.pageNum,newRIDslotNum=newRid.slotNum;
        	memcpy(curPage+compactEndOffset,&newRIDpageNum,SHORT_BYTE);
        	memcpy(curPage+compactEndOffset+SHORT_BYTE,&newRIDslotNum,SHORT_BYTE);
        	//update things(leng:-1,emptySpaceOffset,slotOffset)
        	updateSlotOffset(curPage,rid.slotNum,compactEndOffset);
        	updateRecordLength(curPage,rid.slotNum,-1);

        	short newlen=getRecordLength(curPage,rid.slotNum);
//        	cout<<"new record len="<<newlen<<endl;

        	updateEmptySpaceOffset(curPage,compactEndOffset+TOMB_BYTE);
        }
    }


    fileHandle.writePage(rid.pageNum,curPage,SystemManager);

    free(myData);

    free(curPage);

	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
	unsigned char* curPage=(unsigned char*)malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum,curPage,SystemManager);
	short recordOffset=getRecordOffset(curPage,rid.slotNum);
	if(recordOffset==-1){
		assert(false && "trying to delete a already deleted record");
		return -1;
	}
	short recordLen=getRecordLength(curPage,rid.slotNum);
	RID tombRID;
	if(recordLen==-1)getTombRID(curPage,rid.slotNum,tombRID);
	//compact
	unsigned short compactEndOffset=compactPage(curPage,recordOffset);
	//update (offset:-1)
	updateSlotOffset(curPage,rid.slotNum,-1);
	updateRecordLength(curPage,rid.slotNum,0);
	updateEmptySpaceOffset(curPage,compactEndOffset);
	fileHandle.writePage(rid.pageNum,curPage,SystemManager);
	free(curPage);
	if(recordLen==-1)return deleteRecord(fileHandle,recordDescriptor,tombRID);
	else return 0;
}
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
	int n;
	for(int i=0;i<recordDescriptor.size();i++){
		if(recordDescriptor[i].name==attributeName){
			n=i;
			break;
		}
	}
	unsigned char* curPage=(unsigned char*)malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum,curPage,SystemManager);
	unsigned short recordOffset=getRecordOffset(curPage,rid.slotNum);

	if(isTomb(curPage,rid.slotNum)){
		RID tombRID;
		getTombRID(curPage,rid.slotNum,tombRID);
		free(curPage);
		return readAttribute(fileHandle,recordDescriptor,tombRID,attributeName,data);
	}

	int actualByteForNullsIndicator=getActualByteForNullsIndicator(1);
	char* nullIndicator=(char*)malloc(actualByteForNullsIndicator);

	memset (nullIndicator,0,actualByteForNullsIndicator);
	if(getNthNullBit(n,curPage+recordOffset+SHORT_BYTE)==1)setNthNullBitToOne(nullIndicator,0);

    unsigned char* nthAttributeData=(unsigned char*)malloc(PAGE_SIZE);
    unsigned short nthAttributeDataSize;
	convertNthAttribute(curPage+recordOffset,nthAttributeData,nthAttributeDataSize,n,recordDescriptor);
	memcpy(data,nullIndicator,actualByteForNullsIndicator);
	memcpy((char*)data+actualByteForNullsIndicator,nthAttributeData,nthAttributeDataSize);
	free(nullIndicator);

	free(nthAttributeData);
	free(curPage);
	return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator){

	rbfm_ScanIterator.fileHandle.curFileName=fileHandle.curFileName;
	rbfm_ScanIterator.fileHandle.openFile(rbfm_ScanIterator.fileHandle.curFileName);

	rbfm_ScanIterator.recordDescriptor=recordDescriptor;
	rbfm_ScanIterator.conditionAttribute=conditionAttribute;

	for(int i=0;i<recordDescriptor.size();i++){
		rbfm_ScanIterator.attributeToNumMap[recordDescriptor[i].name]=i;
	}
	rbfm_ScanIterator.conditionAttributeNum=rbfm_ScanIterator.attributeToNumMap[conditionAttribute];


	rbfm_ScanIterator.compOp=compOp;
	rbfm_ScanIterator.value= (unsigned char*)malloc(PAGE_SIZE);

	if(!value){rbfm_ScanIterator.value=NULL;}
	else if(recordDescriptor[rbfm_ScanIterator.conditionAttributeNum].type==TypeInt || recordDescriptor[rbfm_ScanIterator.conditionAttributeNum].type==TypeReal){

		memcpy(rbfm_ScanIterator.value,value,INT_BYTE);
    }else{

    	int len;
    	memcpy(&len,value,INT_BYTE);
    	memcpy(rbfm_ScanIterator.value,value,INT_BYTE+len);
    }

	rbfm_ScanIterator.attributeNames.clear();
    for(string name : attributeNames)rbfm_ScanIterator.attributeNames.push_back(name);

	rbfm_ScanIterator.scanNullIndicator=(unsigned char*)malloc(getActualByteForNullsIndicator(attributeNames.size()));

	rbfm_ScanIterator.curPageNum=0;
	rbfm_ScanIterator.curSlotNum=0;

	rbfm_ScanIterator.curPage=(unsigned char*)malloc(PAGE_SIZE);



	if(rbfm_ScanIterator.fileHandle.readPage(0,rbfm_ScanIterator.curPage,SystemManager)!=0)return -1;
	else return 0;

}
void RBFM_ScanIterator::reset(){
	curPageNum=0;
	curSlotNum=0;
}
RC RBFM_ScanIterator::close() {
    PagedFileManager::instance()->closeFile(fileHandle);
    if(value)free(value);
    if(curPage)free(curPage);
    if(scanNullIndicator)free(scanNullIndicator);

    return 0;
};

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
	//update curPageNum,curSlotNum


	while(fileHandle.readPage(curPageNum,curPage,SystemManager)==0){
		unsigned short slotCount=RecordBasedFileManager::instance()->getSlotCount(curPage);
		for(int i=curSlotNum; i<slotCount; i++){
//			cout<<"curPageNum="<<curPageNum<<",curSlotNum"<<i<<endl;
			short slotOffset=RecordBasedFileManager::instance()->getRecordOffset(curPage,i);
			short recordLen=RecordBasedFileManager::instance()->getRecordLength(curPage,i);
			if(slotOffset==-1 || recordLen==-1){
				continue;
			}
            //determine if the slot meets the condition
			//the num of the conditionAttribute = conditionAttributeNum
           RID tempRID;
           tempRID.pageNum=curPageNum;
           tempRID.slotNum=i;
           unsigned char* readData=(unsigned char*)malloc(PAGE_SIZE);
           RecordBasedFileManager::instance()->readRecord(fileHandle,recordDescriptor,tempRID,readData);
           unsigned char* nthRecord=(unsigned char*)malloc(PAGE_SIZE);
           unsigned short nthRecordSize;
           RecordBasedFileManager::instance()->convertData(recordDescriptor, readData, nthRecordSize, nthRecord, RBF);
           if(RecordBasedFileManager::instance()->getNthNullBit(conditionAttributeNum,nthRecord+SHORT_BYTE)==1
        		   && compOp!=NO_OP)continue;
			//
			unsigned char* nthAttributeData=(unsigned char*)malloc(PAGE_SIZE);
			unsigned short nthAttributeDataSize;


			RecordBasedFileManager::instance()->convertNthAttribute(nthRecord,nthAttributeData,nthAttributeDataSize,conditionAttributeNum,recordDescriptor);
			if(RecordBasedFileManager::instance()->compare(recordDescriptor[conditionAttributeNum].type,nthAttributeData,compOp,(unsigned char*)value,nthAttributeDataSize)){

				unsigned short dataSize;
            	RecordBasedFileManager::instance()->convertScanData(recordDescriptor,attributeNames,attributeToNumMap,nthRecord,dataSize,(unsigned char*)data);

//            	unsigned char* test1=(unsigned char*)malloc(3);
//				memcpy(test1,(unsigned char*)data+1+4,3);
//				cout<<"test1="<<test1<<endl;
//                cout<<"yes"<<endl;
            	rid.pageNum=curPageNum;
            	rid.slotNum=i;
            	//curPageNum=curPageNum;
            	curSlotNum=i+1;
            	free(readData);
            	free(nthRecord);
            	free(nthAttributeData);
            	return 0;
            }
            else {
//            	cout<<"no"<<endl;
            	free(readData);
            	free(nthRecord);
            	free(nthAttributeData);
            	continue;
            }


		}
		curSlotNum=0;
		curPageNum++;
	}

	return RBFM_EOF;
}

//-----------helper functions---------------------

//typedef enum { EQ_OP = 0, // no condition// =
//           LT_OP,      // <
//           LE_OP,      // <=
//           GT_OP,      // >
//           GE_OP,      // >=
//           NE_OP,      // !=
//           NO_OP       // no condition
bool RecordBasedFileManager::compare(const AttrType attrType,const unsigned char* data1 ,const CompOp compOp, const unsigned char* data2,const unsigned short dataSize){
//    cout<<"in compare,attrType="<<attrType<<endl;

	if(compOp==NO_OP)return true;
	int i1,i2;
	memcpy(&i1,data1,INT_BYTE);
	memcpy(&i2,data2,INT_BYTE);


	float f1,f2;
	memcpy(&f1,data1,INT_BYTE);
	memcpy(&f2,data2,FLOAT_BYTE);

	string s1,s2;
	if(attrType==TypeVarChar){
//		cout<<"stringstring"<<endl;
		int s1Len,s2Len;
		memcpy(&s1Len,data1,INT_BYTE);
		memcpy(&s2Len,data2,INT_BYTE);
		char* temp1=(char*)malloc(s1Len);
		char* temp2=(char*)malloc(s2Len);
		memcpy(temp1,data1+INT_BYTE,s1Len);
		memcpy(temp2,data2+INT_BYTE,s2Len);
		s1=string(temp1,s1Len);
		s2=string(temp2,s2Len);
        free(temp1);
        free(temp2);
	}

	if(compOp==EQ_OP){
//        cout<<"dataSize="<<dataSize<<endl;
		return memcmp(data1,data2,dataSize)==0;
	}
	else if(compOp==NE_OP)return memcmp(data1,data2,dataSize)!=0;
	else if(compOp==LT_OP){
		if(attrType==TypeInt){

			return i1<i2;
		}else if(attrType==TypeReal){
			return f1<f2;
		}else{
			return s1<s2;
		}
	}
	else if(compOp==LE_OP){
		if(attrType==TypeInt){
			return i1<=i2;
		}else if(attrType==TypeReal){
			return f1<=f2;
		}else{
			return s1<=s2;
		}
	}
	else if(compOp==GT_OP){
		if(attrType==TypeInt){
			return i1>i2;
		}else if(attrType==TypeReal){
			return f1>f2;
		}else{
			return s1>s2;
		}
	}
	else if(compOp==GE_OP){
		if(attrType==TypeInt){
			return i1>=i2;
		}else if(attrType==TypeReal){
			return f1>=f2;
		}else{
			return s1>=s2;
		}
	}else
		return false;
}

void RecordBasedFileManager::convertScanData(
		const vector<Attribute> &recordDescriptor,
		const vector<string> &attributeNames,
		map<string,int>& attributeToNumMap,
		const void *myData,
		unsigned short& scanDataSize,
		unsigned char* scanData){

	scanDataSize=0;

    unsigned actualByteForScanDataNullsIndicator=getActualByteForNullsIndicator(attributeNames.size());
    memset(scanData,0,actualByteForScanDataNullsIndicator);
    scanDataSize+=actualByteForScanDataNullsIndicator;

    for(int i=0;i<attributeNames.size();i++){
    	unsigned short attributeNum=attributeToNumMap[attributeNames[i]];
    	unsigned char nthNullBit=getNthNullBit(attributeNum,(unsigned char*)myData+SHORT_BYTE);
    	if(nthNullBit==0){
    		unsigned char* nthAttributeData=(unsigned char*)malloc(PAGE_SIZE);
    		unsigned short nthAttributeDataSize;
    		convertNthAttribute((unsigned char*)myData,nthAttributeData,nthAttributeDataSize,attributeNum,recordDescriptor);

    		memcpy(scanData+scanDataSize,nthAttributeData,nthAttributeDataSize);
    		scanDataSize+=nthAttributeDataSize;
    		free(nthAttributeData);
    	}else{
    		setNthNullBitToOne(scanData,i);
    	}
    }
//    cout<<"scanDataSize="<<scanDataSize<<endl;
    return;

}
void RecordBasedFileManager::convertNthAttribute(unsigned char* myData,
	unsigned char* nthAttributeData,
	unsigned short& nthAttributeDataSize,
	unsigned short n,
	const vector<Attribute> &recordDescriptor
	){

	nthAttributeDataSize=0;
    unsigned short fieldCount=recordDescriptor.size();
    unsigned actualByteForNullsIndicator=getActualByteForNullsIndicator(fieldCount);
	unsigned short start,end;

	const unsigned char* temp=(unsigned char*)myData;
    if(getNthNullBit(n,temp+SHORT_BYTE)==1){
    	nthAttributeDataSize=0;
    	return;
    }

	Attribute a=recordDescriptor[n];
	unsigned short startOffset,endOffset;
	unsigned short inRecordOffset;
	if(n==0)startOffset=SHORT_BYTE+actualByteForNullsIndicator+SHORT_BYTE*fieldCount;
	else {
		memcpy(&inRecordOffset,(unsigned char*)myData+SHORT_BYTE+actualByteForNullsIndicator+SHORT_BYTE*(n-1),SHORT_BYTE);
		startOffset=SHORT_BYTE+actualByteForNullsIndicator+SHORT_BYTE*fieldCount+inRecordOffset;
	}

	memcpy(&inRecordOffset,(unsigned char*)myData+SHORT_BYTE+actualByteForNullsIndicator+SHORT_BYTE*(n),SHORT_BYTE);
	endOffset=SHORT_BYTE+actualByteForNullsIndicator+SHORT_BYTE*fieldCount+inRecordOffset;

	int len=endOffset-startOffset;

	if(a.type==TypeVarChar){
        memcpy(nthAttributeData+nthAttributeDataSize,&len,INT_BYTE);
        nthAttributeDataSize+=INT_BYTE;
	}



	memcpy(nthAttributeData+nthAttributeDataSize,(unsigned char*)myData+startOffset,len);
	nthAttributeDataSize+=len;
}


//page
short RecordBasedFileManager::compactPage(unsigned char* curPage, short compactStartPos){

	unsigned short slotCount=getSlotCount(curPage);
	vector<pair<unsigned short,short>> v;

	for(int i=0;i<slotCount;i++){
		short recordOffset=getRecordOffset(curPage,i);
		if(recordOffset>compactStartPos){

			v.push_back(pair<unsigned short,short>(i,recordOffset));
		}
	}
    sort(v.begin(),v.end(),[](pair<unsigned short,short> a, pair<unsigned short,short> b){
        return a.second<b.second;
    });

    while(v.size()>0){
    	short recordSize=getRecordLength(curPage,v[0].first);
    	if(recordSize==-1)recordSize=SHORT_BYTE*2;
    	short recordOffset=getRecordOffset(curPage,v[0].first);
    	//move record to the its new corresponding position
    	unsigned char* temp=(unsigned char*)malloc(recordSize);
    	memcpy(temp,curPage+recordOffset,recordSize);
    	memcpy(curPage+compactStartPos,temp,recordSize);
    	//update slotOffset in slotDir
    	updateSlotOffset(curPage,v[0].first,compactStartPos);
    	compactStartPos+=recordSize;
    	v.erase(v.begin());
    	free(temp);
    }
    return compactStartPos;
}

unsigned short RecordBasedFileManager::emptySpaceCount(unsigned char* page){
	unsigned short startOfEmptySpace=getStartOfEmptySpace(page);
	unsigned slotDirSpaceCount=getSlotDirSpaceCount(page);
	return PAGE_SIZE-startOfEmptySpace-slotDirSpaceCount;


}
unsigned short RecordBasedFileManager::getStartOfEmptySpace(unsigned char* page){
	unsigned short startOfEmptySpace;
	memcpy(&startOfEmptySpace,page+PAGE_SIZE-SHORT_BYTE,SHORT_BYTE);
	return startOfEmptySpace;
}




void RecordBasedFileManager::updateEmptySpaceOffset(unsigned char* page,unsigned short newValue){
	memcpy(page+PAGE_SIZE-SHORT_BYTE,&newValue,SHORT_BYTE);
}



//RECORD
short RecordBasedFileManager::getRecordOffset(const unsigned char* page,unsigned slotNum){
    short ridOffset;
    unsigned short slotOffset=getNthSlotStartPos(page,slotNum);
    memcpy(&ridOffset,page+slotOffset,SHORT_BYTE);
    return ridOffset;
}

short RecordBasedFileManager::getRecordLength(const unsigned char* curPage,unsigned slotNum){
	short ans;
	memcpy(&ans,(unsigned char*)curPage+getNthSlotStartPos(curPage,slotNum)+SHORT_BYTE,SHORT_BYTE);

	return ans;
}

bool RecordBasedFileManager::isTomb(unsigned char* page, unsigned slotNum){
	return getRecordLength(page,slotNum)==-1;
}

//SLOT
//slotNum starts from 0  (offset,length)

unsigned short RecordBasedFileManager::getNthSlotStartPos(const unsigned char* data,int slotNum){
    short ans=PAGE_SIZE-SHORT_BYTE*2-SLOT_BYTE*(slotNum+1);
    return ans;
}
void RecordBasedFileManager::updateSlotOffset(unsigned char* curPage,unsigned short slotNum, short newOffset){
	unsigned short nthSlotStartPos=getNthSlotStartPos(curPage,slotNum);
    memcpy(curPage+nthSlotStartPos,&newOffset,SHORT_BYTE);
}
void RecordBasedFileManager::updateSlotCount(unsigned char* page,unsigned short newValue){
	memcpy(page+PAGE_SIZE-SHORT_BYTE*2,&newValue,SHORT_BYTE);
}
unsigned short RecordBasedFileManager::getSlotCount(unsigned char* page){
	unsigned short slotCount;
	memcpy(&slotCount,page+PAGE_SIZE-SHORT_BYTE*2,SHORT_BYTE);
	return slotCount;
}
unsigned short RecordBasedFileManager::getSlotDirSpaceCount(unsigned char* curPage){
	unsigned short slotCount=getSlotCount(curPage);
	return SHORT_BYTE*2+slotCount*SLOT_BYTE;
}

//get
short RecordBasedFileManager::getFirstUnusedSlot(unsigned char* page){
	unsigned short slotCount=getSlotCount(page);
	for(int i=0;i<slotCount;i++){
	    if(getRecordOffset(page,i)==-1){
	    	return i;
	    }
	}
	return -1;
}
void RecordBasedFileManager::getTombRID(unsigned char* page,unsigned slotNum,RID& tombRID){
	unsigned short recordOffset=getRecordOffset(page,slotNum);
	unsigned short tombRIDPage,tombRIDSlotNum;
	memcpy(&tombRIDPage,page+recordOffset,SHORT_BYTE);
	memcpy(&tombRIDSlotNum,page+recordOffset+SHORT_BYTE,SHORT_BYTE);
	tombRID.pageNum=tombRIDPage;
	tombRID.slotNum=tombRIDSlotNum;
}
//tomb
short RecordBasedFileManager::getFirstTomb(unsigned char* page){
	unsigned short slotCount=getSlotCount(page);
	for(int i=0;i<slotCount;i++){
	    if(getRecordLength(page,i)==-1 && getRecordOffset(page,i)!=-1){
	    	return i;
	    }
	}
	return -1;
}

//update

void RecordBasedFileManager::updateRecordLength(unsigned char* page, unsigned slotNum,short newVal){
	unsigned short nthSlotStartPos=getNthSlotStartPos(page,slotNum);
    memcpy(page+nthSlotStartPos+SHORT_BYTE,&newVal,SHORT_BYTE);
}


//debug tools
void RecordBasedFileManager::printBitsOfOneByte(unsigned char c){
	  for(int i=0;i<8;i++){
		  unsigned char mask=1;
		  if(c & (mask << (7-i)))cout<<"1";
		  else cout<<"0";
	  }
	  cout<<"  ";
}
void RecordBasedFileManager::converDataToBits(void* data, int size){
	unsigned char* temp=(unsigned char*)data;
	for(int i=0;i<size;i++){
		printBitsOfOneByte(*(temp+i));

	}
	cout<<endl;
}




