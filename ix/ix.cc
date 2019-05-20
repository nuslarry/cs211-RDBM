
#include "ix.h"

RC IXFileHandle::readPage(PageNum pageNum, void *data){
    return myFileHandle.readPage(pageNum,data);
}
RC IXFileHandle::writePage(PageNum pageNum, void *data){
    return myFileHandle.writePage(pageNum,data);
}
RC IXFileHandle::appendPage(const void *data){
    return myFileHandle.appendPage(data);
}

FileHandle& IXFileHandle::getMyFileHandle(){
    return myFileHandle;
}
RC IXFileHandle::updateRootLeafpage(int newRootLeafPage){
    myFileHandle.rootLeafPage=newRootLeafPage;
}
bool IXFileHandle::fileExists(const string &fileName){
    ifstream myIfs(fileName);
    return myIfs.good();
}
//void* IXFileHandle::createEmptyIndexPage(void* page){
//    initializeEmptyIndexPage(page);
//    return ;
//}

int IXFileHandle::rootLeafPage(){
    return myFileHandle.rootLeafPage;
}
int IXFileHandle::getNumberOfPages()
{
    return myFileHandle.curPageNum;
}


IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return RecordBasedFileManager::instance()->createFile(fileName);;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return RecordBasedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    ixfileHandle.curFileName=fileName;
    return RecordBasedFileManager::instance()->openFile(fileName,ixfileHandle.getMyFileHandle());
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return RecordBasedFileManager::instance()->closeFile(ixfileHandle.getMyFileHandle());
}

void IndexManager::initializeEmptyIndexPage(void* emptyHeaderPage){
    short freeSpaceOffset=0;
    short slotCount=0;
    int nextLeafPage=-1;
    memcpy((char*)emptyHeaderPage+FREE_SPACE_OFFSET,&freeSpaceOffset,sizeof(short));
    memcpy((char*)emptyHeaderPage+SLOT_COUNT_OFFSET,&slotCount,sizeof(short));
    memcpy((char*)emptyHeaderPage+NEXT_LEAF_PAGE_OFFSET,&nextLeafPage,sizeof(int));
    return;
}

short IndexManager::getFreeSpaceOffset(void* page){
    short ans;
    memcpy(&ans,(char*)page+FREE_SPACE_OFFSET,SHORT_BYTE);
    return ans;
}

short IndexManager::getIndexPageFreeSpace(void* indexPage){
    short emptySpaceOffset,slotCount;
    memcpy(&emptySpaceOffset,(char*)indexPage+FREE_SPACE_OFFSET,sizeof(short));
    memcpy(&slotCount,(char*)indexPage+SLOT_COUNT_OFFSET,sizeof(short));
    return RESERVE_OFFSET-emptySpaceOffset-slotCount*SLOT_BYTE;
}
int IndexManager::findFirstPageWithNoIndex(IXFileHandle &ixfileHandle){
    int curPageNum=ixfileHandle.getNumberOfPages();
    //    cout<<"curPageNum="<<curPageNum<<endl;
    for(int i=0;i<curPageNum;i++){
        void* curPage=malloc(PAGE_SIZE);
        memset (curPage,0,PAGE_SIZE);
        ixfileHandle.readPage(i,curPage);
        short freeSpaceOffset=getFreeSpaceOffset(curPage);
        //        cout<<"@@freeSpaceOffset="<<freeSpaceOffset<<endl;
        if(freeSpaceOffset==0)return i;
        free(curPage);
    }
    return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    int* curKey=(int*)malloc(INT_BYTE);
    memcpy(curKey,key,INT_BYTE);
    //    cout<<"pageNum="<<ixfileHandle.getNumberOfPages()<<endl;
    vector<Attribute> recordDescriptor=getRecordDescriptor(attribute);
    int rootPageNum=ixfileHandle.rootLeafPage();
    if(rootPageNum==-1){
        void* emptyPage=malloc(PAGE_SIZE);
        memset (emptyPage,0,PAGE_SIZE);
        initializeEmptyIndexPage(emptyPage);
        int firstPageWithNoIndex=findFirstPageWithNoIndex(ixfileHandle);
        if(firstPageWithNoIndex==-1){
            ixfileHandle.appendPage(emptyPage);
            int lastPageNum=ixfileHandle.getNumberOfPages()-1;
            rootPageNum=lastPageNum;

        }else{
            rootPageNum=firstPageWithNoIndex;
        }
        ixfileHandle.updateRootLeafpage(rootPageNum);
        free(emptyPage);
    }
    //    cout<<"key=";printKey(key,attribute);cout<<endl;
    addReturnInfo ari(insertEntryToNthPage(ixfileHandle, rootPageNum, attribute, key, rid));


    //    cout<<"@@="<<ari.dataSize<<endl;
    if(ari.rt==SUCCESS){
        //cout<<"wtf"<<endl;
        //        void* temp=malloc(PAGE_SIZE);
        //        ixfileHandle.readPage(rootPageNum,temp);
        //        int test;
        //        memcpy(&test,(char*)temp+2+1+2*5+4,INT_BYTE);
        //        cout<<"test="<<test<<endl;
        //        memcpy(&test,(char*)temp+2+1+2*5+4+4,INT_BYTE);
        //        cout<<"test="<<test<<endl;

        return SUCCESS;


    }
    else{
        if(*curKey==4995){
            cout<<"@@2"<<endl;
            cout<<"i am 4995"<<endl;
            int test;
            cin>>test;
        }
        //cout<<" == rootPage="<<ixfileHandle.rootLeafPage()<<endl;
        //void* temp=malloc(PAGE_SIZE);
        //ixfileHandle.readPage(ixfileHandle.rootLeafPage(),temp);
        //cout<<"RootfreeSpace="<<getIndexPageFreeSpace(temp)<<endl;

        int firstPageWithNoIndex=findFirstPageWithNoIndex(ixfileHandle);
        //        cout<<"firstPageWithNoIndex="<<firstPageWithNoIndex<<endl;
        int newRootPageNum;
        void* newRootPage=malloc(PAGE_SIZE);
        memset (newRootPage,0,PAGE_SIZE);
        initializeEmptyIndexPage(newRootPage);
        if(firstPageWithNoIndex==-1){
            ixfileHandle.appendPage(newRootPage);
            int lastPageNum=ixfileHandle.getNumberOfPages()-1;
            newRootPageNum=lastPageNum;
        }else{
            newRootPageNum=firstPageWithNoIndex;
        }
        //        cout<<"newRootPageNum="<<newRootPageNum<<endl;
        //        cout<<"@@1="<<getIndexPageFreeSpace(newRootPage)<<endl;
        insertDataToPageWithSpace(newRootPage, attribute, ari.data,ari.dataSize);
//        cout<<"newRootPageNum="<<newRootPageNum<<endl;
        //        cout<<getLeftChildren(attribute,ari.data)<<","<<getRightChildren(attribute,ari.data)<<endl;

        ixfileHandle.writePage(newRootPageNum,newRootPage);
        ixfileHandle.updateRootLeafpage(newRootPageNum);
        //        printPageI(ixfileHandle,attribute,newRootPageNum);
        free(newRootPage);

    }
    return 0;
}
RC IndexManager::deleteEntryInNthPage(IXFileHandle &ixfileHandle, int n, const Attribute &attribute, const void *key, const RID &rid){
    void* curPage=malloc(PAGE_SIZE);
    memset (curPage,0,PAGE_SIZE);
    ixfileHandle.readPage(n,curPage);
    indexPageType pageType=getIndexPageType(curPage);
    //    if(pageType==Leaf){
    //        cout<<n<<" is leaf"<<endl;
    //    }else cout<<n<<" is index"<<endl;
    if(pageType==Leaf){
        bool foundDeletedEntry=false;
        short slotCount=getSlotCount(curPage);
        short deletePos=-1;
        for(int i=0;i<slotCount;i++){
            short ithRecordOffset=getIthRecordOffset(curPage,i);
            void* tempKey=malloc(PAGE_SIZE);
            memset (tempKey,0,PAGE_SIZE);
            extractKey(tempKey,attribute,(char*)curPage+ithRecordOffset);
            unsigned curRidPageNum=getridPageNum(attribute,(char*)curPage+ithRecordOffset);
            unsigned curRidSlotNum=getridSlotNum(attribute,(char*)curPage+ithRecordOffset);


            short ithRecordLen=getIthRecordLen(curPage,i);
            void* temp=malloc(ithRecordLen);
            memset (temp,0,ithRecordLen);
            RecordBasedFileManager::instance()->setNthNullBitToOne((char*)temp+SHORT_BYTE,3);
            RecordBasedFileManager::instance()->setNthNullBitToOne((char*)temp+SHORT_BYTE,4);
            if(isKeyEqual(attribute,key,tempKey) && curRidPageNum==rid.pageNum && curRidSlotNum==rid.slotNum){
                foundDeletedEntry=true;
                deletePos=i;
                free(tempKey);
                free(temp);
                break;

            }else{
                free(tempKey);
                free(temp);
            }
        }
        //        cout<<"foundDeletedEntry="<<foundDeletedEntry<<endl;
        if(foundDeletedEntry){
            deleteIthRecord(curPage,deletePos);
            slotCount=getSlotCount(curPage);
            ixfileHandle.writePage(n,curPage);
            free(curPage);
            return SUCCESS;
        }
        else{
            int next=getNextLeafPageNum(curPage);
            if(next==-1){
                free(curPage);
                return -1;
            }
            free(curPage);
            return deleteEntryInNthPage(ixfileHandle,next,attribute,key,rid);
        }
    }else{
        int insertPageNum=findInsertPageNum(curPage,attribute,key);
        free(curPage);
        return deleteEntryInNthPage(ixfileHandle,insertPageNum,attribute,key,rid);
    }
}
void IndexManager::deleteIthRecord(void* page, int i){
    short slotCount=getSlotCount(page);
    short ithRecordOffset=getIthRecordOffset(page,i);
    short ithRecordLen=getIthRecordLen(page,i);
    //    void* temp=malloc(PAGE_SIZE);
    //    short tempSize=getFreeSpaceOffset(page)-(ithRecordOffset+ithRecordLen);
    void* temp=malloc(ithRecordLen);
    memset (temp,0,ithRecordLen);
    RecordBasedFileManager::instance()->setNthNullBitToOne((char*)temp+SHORT_BYTE,3);
    RecordBasedFileManager::instance()->setNthNullBitToOne((char*)temp+SHORT_BYTE,4);
    memcpy((char*)page+ithRecordOffset,temp,ithRecordLen);
    free(temp);
    //    memcpy(temp,(char*)page+ithRecordOffset+ithRecordLen,tempSize);
    //    memcpy((char*)page+ithRecordOffset,temp,tempSize);
    //    updateFreeSpaceOffset(page,ithRecordOffset+tempSize);
    //
    //    for(int j=i+1;j<slotCount;j++){
    //        short tempOffset=getIthRecordOffset(page,j);
    //        updateIthSlotOffset(page,j,tempOffset-ithRecordLen);
    //    }
    //    tempSize=SLOT_BYTE*(slotCount-i-1);
    //    memcpy(temp,(char*)page+getIthSlotOffset(slotCount-1),tempSize);
    //    memcpy((char*)page+getIthSlotOffset(slotCount-2),temp,tempSize);
    //    updateSlotCount(page,slotCount-1);
    return;
}
addReturnInfo IndexManager::insertEntryToNthPage(IXFileHandle &ixfileHandle, int n, const
                                                 Attribute &attribute, const void *key, const RID &rid){
    int* curKey=(int*)malloc(INT_BYTE);
    memcpy(curKey,key,INT_BYTE);
    void* curPage=malloc(PAGE_SIZE);
    memset (curPage,0,PAGE_SIZE);
    ixfileHandle.readPage(n,curPage);
    //    void* curPageCopy=malloc(PAGE_SIZE);
    //    memcpy(curPageCopy,curPage,PAGE_SIZE);
    indexPageType pageType=getIndexPageType(curPage);

    short freeSpace=getIndexPageFreeSpace(curPage);
    if(pageType==Leaf){
        //        cout<<"totalpage="<<ixfileHandle.getNumberOfPages()<<endl;
//                cout<<"leaf="<<n<<endl;
        void* leafData=malloc(PAGE_SIZE);
        memset (leafData,0,PAGE_SIZE);
        unsigned short leafDataSize;
        createLeafData(leafData,leafDataSize,attribute,key,rid);
        if(freeSpace>=leafDataSize){
            int testgg;memcpy(&testgg,(char*)leafData+KEY_OFFSET,INT_BYTE);
            //            cout<<"testgg="<<testgg<<endl;
            //            cout<<"wpw="<<endl;
            //            printKey(key,attribute);cout<<endl;
            //            printKey((char*)leafData+KEY_OFFSET,attribute);cout<<endl;
            //            cout<<"@@2="<<getIndexPageFreeSpace(curPage)<<endl;
            addReturnInfo ari(insertDataToPageWithSpace(curPage,attribute,leafData,leafDataSize));
            //            cout<<"wpw"<<endl;
            ixfileHandle.writePage(n,curPage);
            //            cout<<"n="<<n<<endl;
            //            int test;
            //            memcpy(&test,(char*)curPage+2+1+2*5+4,INT_BYTE);
            //            cout<<"test="<<test<<endl;
            //            memcpy(&test,(char*)curPage+2+1+2*5+4+4,INT_BYTE);
            //            short slotCount=getSlotCount(curPage);
            //            for(int i=0;i<slotCount;i++){
            //                void* tempKey=malloc(PAGE_SIZE);
            //                extractKey(tempKey,attribute,(char*)curPage+getIthRecordOffset(curPage,i));
            //
            //                printKey(tempKey,attribute);cout<<",";
            //            }
            //            cout<<endl;
            //            cout<<"test="<<test<<endl;
            free(leafData);
            free(curPage);
            //            free(curPageCopy);
            return ari;

        }else{
//                        cout<<"wow split=";printKey(key,attribute);cout<<endl;
            //create a function named slpitpage
            addReturnInfo temp(splitPage(n,curPage,ixfileHandle,attribute,leafData,leafDataSize));
            free(curPage);
            //            free(curPageCopy);
            free(leafData);
            return temp;
        }


    }else{//pageType==Index

//                cout<<"index="<<n<<endl;
//        if(*curKey=4995)printPageI(ixfileHandle, attribute,n);
        //        short test=getIndexPageFreeSpace(curPageCopy);
        int insertPageNum=findInsertPageNum(curPage,attribute,key);
        //cout<<"insertPageNum="<<insertPageNum<<endl;

        addReturnInfo ari(insertEntryToNthPage(ixfileHandle,insertPageNum,attribute,key,rid));
        //        cout<<"omfg="<<test<<endl;
        //        cout<<"omfg="<<getIndexPageFreeSpace(curPageCopy)<<endl;
        //        memcpy(curPage,curPageCopy,PAGE_SIZE);
        //        cout<<"@@3="<<getIndexPageFreeSpace(curPage)<<endl;
        if(ari.rt==SUCCESS){
            free(curPage);
            //            free(curPageCopy);
            return ari;
        }
        else{
            if(freeSpace>=ari.dataSize+SLOT_BYTE){
                //                cout<<"n="<<n<<",ari.left="<<getLeftChildren(attribute,ari.data)<<"ari.right="<<getRightChildren(attribute,ari.data)<<endl;
                short slotCount=getSlotCount(curPage);
                //                cout<<"before : freeSpace="<<freeSpace<<endl;
                //                for(int i=0;i<slotCount;i++){
                //                        cout<<"i="<<i<<",leftkid="<<
                //                                getLeftChildren(attribute,(char*)curPage+getIthRecordOffset(curPage,i))<<",rightkid="<<
                //                                getRightChildren(attribute,(char*)curPage+getIthRecordOffset(curPage,i))<<endl;
                //                }
                //                cout<<"have space"<<endl;
                //                cout<<"!aridatasize="<<ari.dataSize<<endl;
                short test=ari.dataSize;
                //                cout<<"test="<<test<<endl;
                //cout<<"insertPageNum is "<< insertPageNum<<endl;
                //cout<<"curpage is "<< n<<endl;
                //                cout<<"@@3="<<getIndexPageFreeSpace(curPage)<<endl;
                //                cout<<"original freespace="<<freeSpace<<endl;
                addReturnInfo temp(insertDataToPageWithSpace(curPage,attribute,ari.data,ari.dataSize));

                //                                cout<<"after : freeSpace="<<freeSpace<<endl;
                //cout<<"n="<<n<<",ari.left="<<getLeftChildren(attribute,ari.data)<<"ari.right="<<getRightChildren(attribute,ari.data)<<endl;
                ixfileHandle.writePage(n,curPage);
                free(curPage);
                //                free(curPageCopy);
                return temp;

            }else{
                //                cout<<"no more freeSpace="<<freeSpace<<endl;
                //create a function named slpitpage]
                addReturnInfo temp(splitPage(n,curPage,ixfileHandle,attribute,ari.data,ari.dataSize));
                free(curPage);
                //                free(curPageCopy);
                return temp;
            }

        }
    }

}
void IndexManager::printPageI(IXFileHandle &ixfileHandle, const Attribute &attribute,int i){

    void* page2=malloc(PAGE_SIZE);
    memset (page2,0,PAGE_SIZE);
    ixfileHandle.readPage(i,page2);
    short slotCount=getSlotCount(page2);
    cout<<"slotCount="<<slotCount<<endl;
    cout<<"freeSpace="<<getIndexPageFreeSpace(page2);
    for(int i=0;i<slotCount;i++){
        short ithRecordOffset=getIthRecordOffset(page2,i);
        cout<<"key:";printKey((char*)page2+ithRecordOffset+KEY_OFFSET,attribute);
        cout<<",leftChild:"<<getLeftChildren(attribute,(char*)page2+ithRecordOffset);
        cout<<",rightChild<<"<<getRightChildren(attribute,(char*)page2+ithRecordOffset)<<endl;
    }
    cout<<"-------"<<endl;
    free(page2);
    return;
}
void IndexManager::updateIthRecordLeftChild(const Attribute &attribute,void* page,int i,int newVal){
    short ithRecordOffset=getIthRecordOffset(page,i);
    short ithRecordLen=getIthRecordLen(page,i);
    short ithRecordLeftChildOffset=ithRecordOffset+ithRecordLen-INT_BYTE*2;
    memcpy((char*)page+ithRecordLeftChildOffset,&newVal,INT_BYTE);
    return;
}
void IndexManager::updateIthRecordRightChild(const Attribute &attribute,void* page,int i,int newVal){
    short ithRecordOffset=getIthRecordOffset(page,i);
    short ithRecordLen=getIthRecordLen(page,i);
    short ithRecordRightChildOffset=ithRecordOffset+ithRecordLen-INT_BYTE;
    memcpy((char*)page+ithRecordRightChildOffset,&newVal,INT_BYTE);
    return;
}
addReturnInfo IndexManager::splitPage(int n, void* page, IXFileHandle &ixfileHandle, const Attribute &attribute, void* data, short dataSize){

    indexPageType pageType=getIndexPageType(page);

    int firstPageWithNoIndex=-1;
    short slotCount=getSlotCount(page);
    //    cout<<"split "<<n<<",slotcount="<<slotCount<<endl;
    if(ixfileHandle.rootLeafPage()==2){
        //        cout<<"firstPageWithNoIndex="<<firstPageWithNoIndex<<endl;
        //        printPageI(ixfileHandle,attribute,n);

    }
    bool foundPos=false;
    for(short i=0;i<slotCount;i++){
        short ithIndexRecordOffset=getIthRecordOffset(page,i);
        if(isKeySmaller(attribute,(char*)data+KEY_OFFSET,(char*)page+ithIndexRecordOffset+KEY_OFFSET)){ // +1 because of null indicator
            //            cout<<"insert at "<<i<<endl;
            foundPos=true;
            if(pageType==Index){
                if(i-1>=0){
                    int val=getLeftChildren(attribute,data);
                    //                    cout<<"val1="<<val<<endl;
                    updateIthRecordRightChild(attribute,page,i-1,val);
                    //                    cout<<"val2="<<val<<endl;
                }
                int val=getRightChildren(attribute,data);
                updateIthRecordLeftChild(attribute,page,i,val);
            }

            break;
        }
    }
    if(pageType==Index){
        if(!foundPos){
            int val=getLeftChildren(attribute,data);
            //            cout<<"val3="<<val<<endl;
            updateIthRecordRightChild(attribute,page,slotCount-1,val);
        }

    }


    //    cout<<"firstPageWithNoIndex="<<firstPageWithNoIndex<<endl;
    void* page1=malloc(PAGE_SIZE);
    memset(page1,0,PAGE_SIZE);
    initializeEmptyIndexPage(page1);
    void* page2=malloc(PAGE_SIZE);
    memset (page2,0,PAGE_SIZE);
    if(firstPageWithNoIndex==-1){
        initializeEmptyIndexPage(page2);
        ixfileHandle.appendPage(page2);
        //        cout<<"current total page="<<ixfileHandle.getNumberOfPages()<<endl;
        firstPageWithNoIndex=ixfileHandle.getNumberOfPages()-1;
    }else{
        initializeEmptyIndexPage(page2);
    }
    //    cout<<"page1 initial freespace="<<getIndexPageFreeSpace(page1)<<endl;
    //    cout<<"page2 initial freespace="<<getIndexPageFreeSpace(page2)<<endl;
    //    cin.get();
    //cout<<"firstPageWithNoIndex="<<firstPageWithNoIndex<<endl;
    void* indexData=malloc(PAGE_SIZE);
    memset (indexData,0,PAGE_SIZE);
    unsigned short indexDataSize;

    int i=0,j=0;
    int total=slotCount+1;
    int mid=total/2;
    bool isDataInserted=false;
    while(i<total){
        void* insertPage;
        int insertPageNum;
        if(i<total/2){
            insertPage=page1;

        }
        else {
            if(i==mid)initializeEmptyIndexPage(page2);
            insertPage=page2;
        }
        //        cout<<"= =i am freespace="<<getIndexPageFreeSpace(page1)<<endl;
        //        cout<<"= =i am freespace="<<getIndexPageFreeSpace(page2)<<endl;
//                cout<<"offset="<<getIthRecordOffset(page,j)<<endl;
        if(j<slotCount && isKeySmaller(attribute,(char*)data+KEY_OFFSET,(char*)page+getIthRecordOffset(page,j)+KEY_OFFSET) && !isDataInserted){
//                        cout<<"###"<<endl;
            if(!(i==mid && pageType==Index)){
//                                cout<<"@@4="<<getIndexPageFreeSpace(insertPage)<<endl;
                insertDataToPageWithSpace(insertPage,attribute,data,dataSize);
                isDataInserted=true;
            }
            if(i==mid){
                void* key=malloc(PAGE_SIZE);
                memset (key,0,PAGE_SIZE);
                extractKey(key,attribute,data);
                createIndexData(indexData,indexDataSize,attribute,key,n,firstPageWithNoIndex);
                free(key);
            }

        }else{
//            cout<<"###2"<<endl;
            if(!(i==mid && pageType==Index) && j<slotCount){
//                                cout<<"@@5="<<getIndexPageFreeSpace(insertPage)<<",len="<<getIthRecordLen(page,j)<<endl;
                insertDataToPageWithSpace(insertPage,attribute,(char*)page+getIthRecordOffset(page,j),getIthRecordLen(page,j));

            }
            if(i==mid){
                void* key=malloc(PAGE_SIZE);
                memset (key,0,PAGE_SIZE);
                extractKey(key,attribute,(char*)page+getIthRecordOffset(page,j));
                createIndexData(indexData,indexDataSize,attribute,key,n,firstPageWithNoIndex);

                free(key);
            }
            j++;

        }
        i++;
        //        cout<<"i am freespace="<<getIndexPageFreeSpace(page1)<<endl;
        //        cout<<"i am freespace="<<getIndexPageFreeSpace(page2)<<endl;
    }


    if(pageType==Leaf){
        int originalNextLeafPage=getNextLeafPageNum(page);
        updatedateNextLeafPage(page2,originalNextLeafPage);
        updatedateNextLeafPage(page1,firstPageWithNoIndex);

    }

    //    cout<<"final i am freespace="<<getIndexPageFreeSpace(page1)<<endl;
    if(!isDataInserted){
        if(isKeySmaller(attribute,(char*)data+KEY_OFFSET,(char*)indexData+KEY_OFFSET))
            insertDataToPageWithSpace(page1,attribute,data,dataSize);
        else
            insertDataToPageWithSpace(page2,attribute,data,dataSize);
    }

    ixfileHandle.writePage(n,page1);
    //    cout<<"final i am freespace="<<getIndexPageFreeSpace(page2)<<endl;
    ixfileHandle.writePage(firstPageWithNoIndex,page2);

    //    cout<<"curPage"<<n<<",next leaf page="<<firstPageWithNoIndex<<endl;
    //    cout<<"left="<<getLeftChildren(attribute,indexData)<<",right="<<getRightChildren(attribute,indexData)<<endl;
    //    printAllLeafPage(ixfileHandle,attribute);
    //    cout<<"firstPageWithNoIndex="<<firstPageWithNoIndex<<endl;
    //cout<<"n="<<n<<endl;
    //cout<<"current total page="<<ixfileHandle.getNumberOfPages()<<endl;
    //        printPageI(ixfileHandle,attribute,2);


    //    std::cin.get();
    //    cout<<"ssfinal i am freespace="<<getIndexPageFreeSpace(page1)<<"page 1 is"<<n<<endl;
    //    cout<<"ssfinal i am freespace="<<getIndexPageFreeSpace(page2)<<"page 1 is"<<firstPageWithNoIndex<<endl;
    free(page1);
    free(page2);
    //    cout<<"indexDataSize="<<indexDataSize<<endl;
    //    cout<<"midKey=";printKey((char*)indexData+KEY_OFFSET,attribute);cout<<endl;
    addReturnInfo temp(addReturnInfo(FULL,indexData,indexDataSize));
    free(indexData);
    return temp;

}
void IndexManager::printAllLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute){

    int curLeafPageNum=0;
    void* page=malloc(PAGE_SIZE);
    memset (page,0,PAGE_SIZE);

    while(curLeafPageNum>=0){
        cout<<"leafPage:"<<curLeafPageNum<<endl;
        ixfileHandle.readPage(curLeafPageNum,page);

        short slotCount=getSlotCount(page);

		for(int i=0;i<slotCount;i++){
			printKey((char*)page+getIthRecordOffset(page,i)+KEY_OFFSET,attribute);cout<<" :"
			<<getridPageNum(attribute,(char*)page+getIthRecordOffset(page,i))<<","<<
			getridSlotNum(attribute,(char*)page+getIthRecordOffset(page,i));cout<<"  ";

			if(i%6==0)cout<<endl;
		}



        int nextLeafPageNum=getNextLeafPageNum(page);
        if(curLeafPageNum==nextLeafPageNum)break;
        curLeafPageNum=nextLeafPageNum;


    }
    cout<<"printLeafEnd"<<endl;
    free(page);

}
void IndexManager::updatedateNextLeafPage(void* page, int newVal){
    memcpy((char*)page+NEXT_LEAF_PAGE_OFFSET,&newVal,INT_BYTE);

}
void IndexManager::extractKey(void* key, const Attribute &attribute, void* data){
    //    cout<<"wtf"<<endl;
    int offset=DESCRIPTOR_SHORT_BYTE;
    offset+=NULL_DESCRIPTOR_BYTE;
    offset+=POINTER_BYTE;
    if(attribute.type==TypeVarChar){
        int len;
        memcpy(&len,(char*)data+offset,INT_BYTE);
        memcpy(key,(char*)data+offset,INT_BYTE+len);
    }else{
        memcpy(key,(char*)data+offset,INT_BYTE);
    }
    return;
}
void IndexManager::printKey(const void* key, const Attribute &attribute){
    if(!key){cout<<"INF"<<endl;return;}
    if(attribute.type==TypeVarChar){

        int len;

        memcpy(&len,key,INT_BYTE);
        void* temp=malloc(len);
        memcpy(temp,(char*)key+INT_BYTE,len);
        string temp2((char*)temp);
//                if(temp2.size()<5)
        cout<<temp2;
//                else
//                    cout<<temp2.substr(0,5);
        free(temp);
    }else if(attribute.type==TypeInt){
        int temp;
        memcpy(&temp,key,INT_BYTE);
        cout<<temp;
    }else{
        float temp;
        memcpy(&temp,key,INT_BYTE);
        cout<<temp;
    }
}
bool IndexManager::isKeySmaller(const Attribute &attribute, const void* key1, const void* key2){
//    cout<<"h1"<<endl;
    if(isKeyEqual(attribute,key1,key2))return true;

    if(!key1 || !key2)return true;

    if(attribute.type==TypeVarChar){
//        cout<<"h3"<<endl;
        int key1Len,key2Len;
        memcpy(&key1Len,key1,INT_BYTE);
        memcpy(&key2Len,key2,INT_BYTE);

        char* key1Val=(char*)malloc(key1Len);
        char* key2Val=(char*)malloc(key2Len);
//        cout<<"key1Len="<<key1Len<<",key2Len="<<key2Len<<endl;
        memcpy(key1Val,(const char*)key1+INT_BYTE,key1Len);
        memcpy(key2Val,(const char*)key2+INT_BYTE,key2Len);

        string temp1(key1Val),temp2(key2Val);
        //        cout<<"temp1="<<temp1<<",temp2="<<temp2<<endl;
        free(key1Val);
        free(key2Val);
        return temp1<temp2 ? true : false;
    }else if(attribute.type==TypeInt){
//        cout<<"h4"<<endl;
        //        cout<<"~~2"<<endl;
        int key1Val,key2Val;
        memcpy(&key1Val,key1,INT_BYTE);
        memcpy(&key2Val,key2,INT_BYTE);
        return key1Val<key2Val;
    }else{
//        cout<<"h5"<<endl;
        //        cout<<"~~3"<<endl;
        float key1Val,key2Val;
        memcpy(&key1Val,key1,FLOAT_BYTE);
        memcpy(&key2Val,key2,FLOAT_BYTE);
        return key1Val<key2Val;

    }
}
int IndexManager::getLeftChildren(const Attribute &attribute, void* indexRecord){
    int leftChildren;
    int offset=KEY_OFFSET;
    if(attribute.type==TypeVarChar){
        int keyLen;
        memcpy(&keyLen,(char*)indexRecord+offset,INT_BYTE);
        offset+=INT_BYTE;
        offset+=keyLen;
    }else{
        offset+=INT_BYTE;
    }
    memcpy(&leftChildren,(char*)indexRecord+offset,INT_BYTE);
    return leftChildren;
}
int IndexManager::getridPageNum(const Attribute &attribute, void* indexRecord){
    int pageNum;
    int offset=DESCRIPTOR_SHORT_BYTE;
    offset+=NULL_DESCRIPTOR_BYTE;
    offset+=POINTER_BYTE;
    if(attribute.type==TypeVarChar){
        int keyLen;
        memcpy(&keyLen,(char*)indexRecord+offset,INT_BYTE);
        offset+=INT_BYTE;
        offset+=keyLen;
    }else{
        offset+=INT_BYTE;
    }
    memcpy(&pageNum,(char*)indexRecord+offset,INT_BYTE);
    return pageNum;
}
int IndexManager::getRightChildren(const Attribute &attribute, void* indexRecord){
    int rightChildren;
    int offset=DESCRIPTOR_SHORT_BYTE;
    offset+=NULL_DESCRIPTOR_BYTE;
    offset+=POINTER_BYTE;
    if(attribute.type==TypeVarChar){
        int keyLen;
        memcpy(&keyLen,(char*)indexRecord+offset,INT_BYTE);
        offset+=INT_BYTE;
        offset+=keyLen;
    }else{
        offset+=INT_BYTE;
    }
    offset+=INT_BYTE;
    memcpy(&rightChildren,(char*)indexRecord+offset,INT_BYTE);
    return rightChildren;
}
int IndexManager::getridSlotNum(const Attribute &attribute, void* indexRecord){
    int slotNum;
    int offset=DESCRIPTOR_SHORT_BYTE;
    offset+=NULL_DESCRIPTOR_BYTE;
    offset+=POINTER_BYTE;
    if(attribute.type==TypeVarChar){
        int keyLen;
        memcpy(&keyLen,(char*)indexRecord+offset,INT_BYTE);
        offset+=INT_BYTE;
        offset+=keyLen;
    }else{
        offset+=INT_BYTE;
    }
    offset+=INT_BYTE;
    memcpy(&slotNum,(char*)indexRecord+offset,INT_BYTE);
    return slotNum;
}
int IndexManager::findInsertPageNum(void* page, const Attribute& attribute, const void* key){
    //    printKey(key,attribute);
    //    cout<<":"<<endl;
    short slotCount=getSlotCount(page);
    short insertSlotNum=-1;
    //    cout<<"findSlotCount="<<slotCount<<endl;
    for(short i=0;i<slotCount;i++){

        short ithIndexRecordOffset=getIthRecordOffset(page,i);
        //        printKey((char*)page+ithIndexRecordOffset+KEY_OFFSET,attribute);cout<<",";
        if(isKeySmaller(attribute,key,(char*)page+ithIndexRecordOffset+KEY_OFFSET)){
            //            cout<<endl;
            //            cout<<getLeftChildren(attribute,(char*)page+ithIndexRecordOffset)<<endl;
            return getLeftChildren(attribute,(char*)page+ithIndexRecordOffset);
        }
    }

    if(insertSlotNum==-1){
        //        cout<<endl;
        //        cout<<getRightChildren(attribute,(char*)page+getIthRecordOffset(page,slotCount-1))<<endl;
        return getRightChildren(attribute,(char*)page+getIthRecordOffset(page,slotCount-1));
    }
}
addReturnInfo IndexManager::insertDataToPageWithSpace(void* page, const Attribute &attribute, void* data,short dataSize){
    //    int test;
    //    memcpy(&test,(char*)data+2+1+2*5+4,INT_BYTE);
    //    cout<<"test="<<test<<endl;
    //    memcpy(&test,(char*)data+2+1+2*5+4+4,INT_BYTE);
    //    cout<<"test="<<test<<endl;

    short freeSpace=getIndexPageFreeSpace(page);
    //    cout<<"freeSpace="<<freeSpace<<",dataSize="<<dataSize<<endl;
    //cout<<"ww"<<endl;
    assert(freeSpace>=dataSize && "freespace should be larger than datasize");
    short freeSpaceOffset=getFreeSpaceOffset(page);
    short slotCount=getSlotCount(page);
    //    cout<<"slotCount="<<slotCount<<endl;
    short insertSlotNum=-1;
    bool foundPos=false;
    for(short i=0;i<slotCount;i++){
        //        cout<<"@@i="<<i<<endl;
        short ithIndexRecordOffset=getIthRecordOffset(page,i);
        //        cout<<"ithIndexRecordOffset="<<ithIndexRecordOffset<<endl;
        if(isKeySmaller(attribute,(char*)data+KEY_OFFSET,(char*)page+ithIndexRecordOffset+KEY_OFFSET)){ // +1 because of null indicator
            insertSlotNum=i;
            if(getIndexPageType(page)==Index){
                foundPos=true;
                if(i-1>=0){
                    int val=getLeftChildren(attribute,data);
                    updateIthRecordRightChild(attribute,page,i-1,val);
                }
                int val=getRightChildren(attribute,data);
                updateIthRecordLeftChild(attribute,page,i,val);
            }



            break;
        }
    }
    if(getIndexPageType(page)==Index){
        if(!foundPos){
            int val=getLeftChildren(attribute,data);
            updateIthRecordRightChild(attribute,page,slotCount-1,val);
        }
    }



    //    cout<<"insertSlotNum="<<insertSlotNum<<",insertKey=";printKey((char*)data+KEY_OFFSET,attribute);cout<<"slotCount="<<slotCount<<endl;
    //    cout<<"freeSpaceOffset="<<freeSpaceOffset<<endl;
    if(insertSlotNum==-1){
        //        cout<<"hahaha"<<endl;
        memcpy((char*)page+freeSpaceOffset,data,dataSize);
        short temp=freeSpaceOffset+dataSize;
        memcpy((char*)page+PAGE_SIZE-SHORT_BYTE*2-INT_BYTE*4-SLOT_BYTE*(slotCount+1),&freeSpaceOffset,SHORT_BYTE);
        memcpy((char*)page+PAGE_SIZE-SHORT_BYTE*2-INT_BYTE*4-SLOT_BYTE*(slotCount+1)+SHORT_BYTE,&dataSize,SHORT_BYTE);
    }else{
        int tempSize;
        void* temp=malloc(PAGE_SIZE);
        memset (temp,0,PAGE_SIZE);
        short insertIndexRecordOffset=getIthRecordOffset(page,insertSlotNum);
        tempSize=getFreeSpaceOffset(page)-insertIndexRecordOffset;
        memcpy(temp,(char*)page+insertIndexRecordOffset,tempSize);
        memcpy((char*)page+insertIndexRecordOffset,data,dataSize);
        memcpy((char*)page+insertIndexRecordOffset+dataSize,temp,tempSize);

        for(int j=insertSlotNum;j<slotCount;j++){
            short recordOffset=getIthRecordOffset(page,j);
            updateIthSlotOffset((char*)page,j,recordOffset+dataSize);
        }
        short temp1=getIthSlotOffset(insertSlotNum)+SLOT_BYTE,temp2=getIthSlotOffset(slotCount-1);
        void* dirMoveLeft=malloc(PAGE_SIZE);
        memset (dirMoveLeft,0,PAGE_SIZE);
        memcpy(dirMoveLeft,(char*)page+temp2,temp1-temp2);
        memcpy((char*)page+temp2-SLOT_BYTE,dirMoveLeft,temp1-temp2);

        memcpy((char*)page+getIthSlotOffset(insertSlotNum),&insertIndexRecordOffset,SHORT_BYTE);
        memcpy((char*)page+getIthSlotOffset(insertSlotNum)+SHORT_BYTE,&dataSize,SHORT_BYTE);
        //        cout<<endl;
        free(temp);
        free(dirMoveLeft);


    }

    //    memcpy(&test,(char*)page+2+1+2*5+4,INT_BYTE);
    //    cout<<"test="<<test<<endl;
    //    memcpy(&test,(char*)page+2+1+2*5+4+4,INT_BYTE);
    //    cout<<"test="<<test<<endl;



    updateFreeSpaceOffset(page,freeSpaceOffset+dataSize);
    updateSlotCount(page,slotCount+1);
    //    cout<<"in inserTofreepage after execution: freespace="<<getIndexPageFreeSpace(page)<<endl;
    //    cout<<"-----"<<endl;
    //    for(int i=0;i<=slotCount;i++){
    //        printKey((char*)page+getIthRecordOffset(page,i)+KEY_OFFSET,attribute);cout<<",";
    //    }
    //    cout<<"----"<<endl;

    return addReturnInfo(SUCCESS,NULL,0);
}
void IndexManager::updateIthSlotOffset(void* page,int i, short newVal){
    memcpy((char*)page+getIthSlotOffset(i),&newVal,SHORT_BYTE);
    return;
}


short IndexManager::getIthRecordOffset(void* page, short i) {
    short ans;
    memcpy(&ans,(char*)page+getIthSlotOffset(i),SHORT_BYTE);
    return ans;
}
short IndexManager::getIthRecordLen(void* page, short i){
    short ans;
    memcpy(&ans,(char*)page+getIthSlotOffset(i)+SHORT_BYTE,SHORT_BYTE);
    return ans;
}
short IndexManager::getIthSlotOffset(short i){
    return RESERVE_OFFSET-SLOT_BYTE*(i+1);
}

void IndexManager::updateFreeSpaceOffset(void* page, short val){
    memcpy((char*)page+FREE_SPACE_OFFSET,&val,SHORT_BYTE);
}
void IndexManager::updateSlotCount(void* page, short val){
    memcpy((char*)page+SLOT_COUNT_OFFSET,&val,SHORT_BYTE);
}

// key  RID(pageNum) RID(slotNum) leftchildren rightchildren
void IndexManager::createLeafData(void* leafData, unsigned short& leafDataSize, const Attribute &attribute, const void *key, const RID &rid){

    void* teacherLeafData=malloc(PAGE_SIZE);
    memset (teacherLeafData,0,PAGE_SIZE);
    //create techerLeafData------start
    char* nullIndicator=(char*)malloc(CHAR_BYTE);
    memset (nullIndicator,0,CHAR_BYTE);
    int offset=0;
    RecordBasedFileManager::instance()->setNthNullBitToOne(nullIndicator,3);
    RecordBasedFileManager::instance()->setNthNullBitToOne(nullIndicator,4);
    memcpy((char*)teacherLeafData+offset,nullIndicator,NULL_DESCRIPTOR_BYTE);
    offset+=NULL_DESCRIPTOR_BYTE;
    if(attribute.type==TypeVarChar){
        int len;
        memcpy(&len,key,INT_BYTE);
        //        cout<<"len="<<len<<endl;
        memcpy((char*)teacherLeafData+offset,key,len+INT_BYTE);
        string test((char*)teacherLeafData+offset+INT_BYTE);
        //    cout<<"test="<<test<<endl;
        offset+=INT_BYTE;
        offset+=len;
    }else{
        memcpy((char*)teacherLeafData+offset,key,INT_BYTE);
        offset+=INT_BYTE;
    }
    memcpy((char*)teacherLeafData+offset,&rid.pageNum,INT_BYTE);
    offset+=INT_BYTE;
    memcpy((char*)teacherLeafData+offset,&rid.slotNum,INT_BYTE);
    offset+=INT_BYTE;
    //create techerLeafData ------end
    vector<Attribute> recordDescriptor=getRecordDescriptor(attribute);
    RecordBasedFileManager::instance()->convertData(recordDescriptor, teacherLeafData,leafDataSize, (unsigned char*)leafData,INDEX);
    free(teacherLeafData);
    free(nullIndicator);
    return;
}

void IndexManager::createIndexData(void* indexData, unsigned short& indexDataSize, const Attribute &attribute, const void *key, int leftChildren, int rightChildren){

    void* teacherIndexData=malloc(PAGE_SIZE);
    memset (teacherIndexData,0,PAGE_SIZE);
    //create techerIndexData------start
    char* nullIndicator=(char*)malloc(CHAR_BYTE);
    memset (nullIndicator,0,CHAR_BYTE);
    //    RecordBasedFileManager::instance()->printBitsOfOneByte(nullIndicator[0]);cout<<endl;
    //    cout<<"hello1"<<endl;
    int offset=0;
    RecordBasedFileManager::instance()->setNthNullBitToOne(nullIndicator,1);
    RecordBasedFileManager::instance()->setNthNullBitToOne(nullIndicator,2);
    //    RecordBasedFileManager::instance()->printBitsOfOneByte(nullIndicator[0]);
    memcpy((char*)teacherIndexData+offset,nullIndicator,NULL_DESCRIPTOR_BYTE);
    offset+=NULL_DESCRIPTOR_BYTE;
    //    cout<<"hello1"<<endl;
    if(attribute.type==TypeVarChar){
        int len;

        memcpy(&len,key,INT_BYTE);
        //        cout<<"len="<<len<<endl;
        memcpy((char*)teacherIndexData+offset,key,len+INT_BYTE);
        offset+=INT_BYTE;
        offset+=len;
    }else{
        memcpy((char*)teacherIndexData+offset,key,INT_BYTE);
        offset+=INT_BYTE;
    }
    memcpy((char*)teacherIndexData+offset,&leftChildren,INT_BYTE);
    offset+=INT_BYTE;
    memcpy((char*)teacherIndexData+offset,&rightChildren,INT_BYTE);
    offset+=INT_BYTE;
    //create techerIndexData ------end
    vector<Attribute> recordDescriptor=getRecordDescriptor(attribute);
    //    cout<<"recordDescriptor.size()="<<recordDescriptor.size()<<endl;
    RecordBasedFileManager::instance()->convertData(recordDescriptor, teacherIndexData,indexDataSize, (unsigned char*)indexData,INDEX);
    //    cout<<"@@left="<<getLeftChildren(attribute,indexData)<<",@@right="<<getRightChildren(attribute,indexData)<<endl;
    free(teacherIndexData);
    free(nullIndicator);
    return;
}
indexPageType IndexManager::getIndexPageType(void* curPage){
    short slotCount=getSlotCount(curPage);
    if(slotCount==0)return Leaf;
    else{
        unsigned char leftChildrenNullBit=RecordBasedFileManager::instance()->getNthNullBit(3,(char*)curPage+DESCRIPTOR_SHORT_BYTE);
        unsigned char rightChildrenNullBit=RecordBasedFileManager::instance()->getNthNullBit(4,(char*)curPage+DESCRIPTOR_SHORT_BYTE);
        if(leftChildrenNullBit==1 && rightChildrenNullBit==1)return Leaf;
        else return Index;
    }
}
short IndexManager::getSlotCount(void* indexPage){
    short slotCount;
    memcpy(&slotCount,(char*)indexPage+SLOT_COUNT_OFFSET,SHORT_BYTE);
    return slotCount;
}

vector<Attribute> IndexManager::getRecordDescriptor(const Attribute &attribute){
    vector<Attribute> ans;
    ans.push_back(attribute);
    Attribute a;
    a.length=4;
    a.name="ridPageNum";
    a.type=TypeInt;
    ans.push_back(a);
    a.name="slotNum";
    ans.push_back(a);
    a.name="leftChildrenNum";
    ans.push_back(a);
    a.name="rightChildrenNum";
    ans.push_back(a);
    return ans;
}



RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    int rootPageNum=ixfileHandle.rootLeafPage();
    //     cout<<"rootPageNum="<<rootPageNum<<endl;
    if(rootPageNum==-1)return -1;
    return deleteEntryInNthPage(ixfileHandle, rootPageNum, attribute, key, rid);
}



void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute){
    cout<<endl;
    //    cout<<"totalpagenum="<<ixfileHandle.getNumberOfPages()<<endl;
    //    cout<<"printBtreeStart"<<endl;
    int rootPageNum=ixfileHandle.rootLeafPage();
    //    printPageI(ixfileHandle,attribute,2);
    printBtreeHelper(ixfileHandle,attribute,rootPageNum,"");
    //    cout<<"printBreeEnd"<<endl;
}
void IndexManager::printBtreeHelper(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum, string emptySpace) {

    cout<<emptySpace<<"{";
    cout<<"\"keys\": [";
    void* page=malloc(PAGE_SIZE);
    memset (page,0,PAGE_SIZE);

    ixfileHandle.readPage(pageNum,page);
    indexPageType pageType=getIndexPageType(page);
    //    cout<<"print"<<endl;
    //    int test;
    //    memcpy(&test,(char*)page+2+1+2*5+4,INT_BYTE);
    //    cout<<"test="<<test<<endl;
    //    memcpy(&test,(char*)page+2+1+2*5+4+4,INT_BYTE);
    //    cout<<"test="<<test<<endl;
    short slotCount=getSlotCount(page);
    vector<int> childrenPages;
    void* prevKey=malloc(PAGE_SIZE);
    memset (prevKey,0,PAGE_SIZE);
    int IthRecordOffset=getIthRecordOffset(page,0);

    extractKey(prevKey,attribute,(char*)page+IthRecordOffset);
    if(pageType==Leaf){
        cout<<"\"";
        printKey(prevKey,attribute);
        cout<<":[";
        cout<<"("<<getLeftChildren(attribute,(char*)page+IthRecordOffset)<<","<<getRightChildren(attribute,(char*)page+IthRecordOffset)<<")";
    }else{
        cout<<"\"";
        printKey(prevKey,attribute);
        cout<<"\"";
        if(slotCount>1)cout<<",";
        int leftChildren=getLeftChildren(attribute,(char*)page+IthRecordOffset);
        //        cout<<"leftChildren="<<leftChildren<<endl;
        childrenPages.push_back(leftChildren);
    }
    for(short i=1;i<slotCount;i++){
        IthRecordOffset=getIthRecordOffset(page,i);
        int leftChildren=getLeftChildren(attribute,(char*)page+IthRecordOffset);
        //        cout<<"leftChildren="<<leftChildren<<endl;
        childrenPages.push_back(leftChildren);
        void* key=malloc(PAGE_SIZE);
        memset (key,0,PAGE_SIZE);
        extractKey(key,attribute,(char*)page+IthRecordOffset);
        if(pageType==Leaf){
            if(isKeyEqual(attribute,key,prevKey)){
                cout<<",";
                cout<<"("<<getLeftChildren(attribute,(char*)page+IthRecordOffset)<<","<<getRightChildren(attribute,(char*)page+IthRecordOffset)<<")";
            } else{
                cout<<"]\",\"";
                printKey(key,attribute);
                cout<<":["<<"("<<getLeftChildren(attribute,(char*)page+IthRecordOffset)<<","<<getRightChildren(attribute,(char*)page+IthRecordOffset)<<")";
            }
            copyKey(attribute,prevKey,key);
        }else{
            cout<<"\"";
            printKey(key,attribute);
            cout<<"\"";
            if(i!=slotCount-1)cout<<",";
        }
        free(key);
    }
    if(pageType==Leaf)cout<<"]\"";
    int rightChildren=getRightChildren(attribute,(char*)page+getIthRecordOffset(page,slotCount-1));
    childrenPages.push_back(rightChildren);
    //cout<<"rightChildren="<<rightChildren<<endl;
    cout<<"]";
    //    cout<<"##slotCount="<<slotCount<<endl;
    //    cout<<"childrenPages.size()="<<childrenPages.size()<<endl;
    if(pageType!=Leaf){

        cout<<","<<endl;;
        cout<<emptySpace<<"\"children\":["<<endl;
        for(int i=0;i<childrenPages.size();i++){
            printBtreeHelper(ixfileHandle,attribute,childrenPages[i],emptySpace+"    ");

            if(i!=childrenPages.size()-1)cout<<","<<endl;
        }
        cout<<endl;
        cout<<emptySpace<<"]";
    }
    cout<<"}";
    free(page);
    free(prevKey);
}
void IndexManager::copyKey(const Attribute & attribute, void* key1, void* key2){ //copy the content in key2 to key1
    if(attribute.type==TypeVarChar){
        int len;
        memcpy(&len,key2,INT_BYTE);
        memcpy(key1,key2,INT_BYTE+len);
    }else{
        memcpy(key1,key2,INT_BYTE);
    }
}
bool IndexManager::isKeyEqual(const Attribute & attribute, const void* key, const void* prevKey){
    if(!key || !prevKey)return false;
    if(attribute.type==TypeVarChar){
        int len1,len2;
        memcpy(&len1,key,INT_BYTE);
        memcpy(&len2,prevKey,INT_BYTE);
        if(len1!=len2)return false;
        if(memcmp((char*)key+INT_BYTE,(char*)prevKey+INT_BYTE,len1)==0)return true;
        else return false;
    }else{
        if(memcmp(key,prevKey,INT_BYTE)==0)return true;
        else return false;
    }
}
IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}
RC IndexManager::scan(IXFileHandle &ixfileHandle,
                      const Attribute &attribute,
                      const void      *lowKey,
                      const void      *highKey,
                      bool            lowKeyInclusive,
                      bool            highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator)
{
    //    cout<<"ggSDF1"<<endl;
    ix_ScanIterator.ixfileHandle=&ixfileHandle;
    //    cout<<"ixfileHandle.curFileName="<<ixfileHandle.curFileName<<endl;
    if(!ixfileHandle.fileExists(ixfileHandle.curFileName)){
        return -1;
    }
    ix_ScanIterator.attribute=attribute;
    if(ix_ScanIterator.attribute.type==TypeVarChar){
        int lowKeyLen;
        if(lowKey){
            memcpy(&lowKeyLen,lowKey,INT_BYTE);
            ix_ScanIterator.lowKey=malloc(INT_BYTE+lowKeyLen);
            memcpy(ix_ScanIterator.lowKey,lowKey,INT_BYTE+lowKeyLen);
        }else ix_ScanIterator.lowKey=NULL;
        if(highKey){
            int highKeyLen;
            memcpy(&highKeyLen,highKey,INT_BYTE);
            ix_ScanIterator.highKey=malloc(INT_BYTE+highKeyLen);
            memcpy(ix_ScanIterator.highKey,highKey,INT_BYTE+highKeyLen);
        }else ix_ScanIterator.highKey=NULL;

    }else{
        if(lowKey){
            ix_ScanIterator.lowKey=malloc(INT_BYTE);
            memcpy(ix_ScanIterator.lowKey,lowKey,INT_BYTE);
        }else ix_ScanIterator.lowKey=NULL;
        if(highKey){
            ix_ScanIterator.highKey=malloc(INT_BYTE);
            memcpy(ix_ScanIterator.highKey,highKey,INT_BYTE);
        }else ix_ScanIterator.highKey=NULL;
    }
    ix_ScanIterator.lowKeyInclusive=lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive=highKeyInclusive;
    ix_ScanIterator.setFirstKey();
    //    printPage2(ixfileHandle,attribute);
    //
    //    printAllLeafPage(ixfileHandle,attribute);
    return SUCCESS;
}
void IX_ScanIterator::setFirstKey(){

    curKeyPage=-1;
    curKeySlotNum=-1;
    int rootPageNum=ixfileHandle->rootLeafPage();
//        cout<<"rootPageNum="<<rootPageNum<<endl;
    setFirstKeyHelper(rootPageNum);
    //    cout<<"firstKey="<<":"<<curKeyPage<<","<<curKeySlotNum<<endl;
    return;
}
void IX_ScanIterator::setFirstKeyHelper(int curPageNum){
//        cout<<"setcurPageNum="<<curPageNum<<endl;
    void* page=malloc(PAGE_SIZE);
    memset (page,0,PAGE_SIZE);
    ixfileHandle->readPage(curPageNum,page);
    indexPageType pageType=IndexManager::instance()->getIndexPageType(page);
    short slotCount=IndexManager::instance()->getSlotCount(page);
    //    cout<<"slotCount="<<slotCount<<endl;
    if(pageType==Leaf){
        for(int i=0;i<slotCount;i++){
            //            cout<<"i-="<<i<<endl;
            //            cout<<"highKey:";IndexManager::instance()->printKey(highKey,attribute);cout<<endl;
            void* key=malloc(PAGE_SIZE);
            memset (key,0,PAGE_SIZE);
            //            cout<<"gg"<<endl;
            short ithRecordOffset=IndexManager::instance()->getIthRecordOffset(page,i);
            //            cout<<"ggasd2="<<ithRecordOffset<<endl;

            IndexManager::instance()->extractKey(key,attribute,(char*)page+ithRecordOffset);

            //            cout<<"~~~=";IndexManager::instance()->printKey(key,attribute);cout<<endl;
            //            cout<<"highKey:";IndexManager::instance()->printKey(highKey,attribute);cout<<endl;
            //            cout<<"WWWW"<<endl;
            //            cout<<"WWWW"<<endl;
            //            cout<<"hello:";IndexManager::instance()->printKey(key,attribute);cout<<endl;
            if(isKeyInRange(key)){
                //                cout<<"yes"<<endl;
                curKeyPage=curPageNum;
                curKeySlotNum=i;
                free(key);
                free(page);
                return;
            }
            free(key);
        }
    }else{
        for(int i=0;i<slotCount;i++){
            void* key=malloc(PAGE_SIZE);
            memset (key,0,PAGE_SIZE);
            short ithRecordOffset=IndexManager::instance()->getIthRecordOffset(page,i);
            IndexManager::instance()->extractKey(key,attribute,(char*)page+ithRecordOffset);
            if(IndexManager::instance()->isKeySmaller(attribute,lowKey,key)){
                int leftChildren=IndexManager::instance()->getLeftChildren(attribute,(char*)page+ithRecordOffset);
                //                cout<<"leftChildren="<<leftChildren<<endl;
                setFirstKeyHelper(leftChildren);
                free(key);
                free(page);
                return;
            }
            free(key);
        }
        short ithRecordOffset=IndexManager::instance()->getIthRecordOffset(page,slotCount-1);
        int rightChildren=IndexManager::instance()->getRightChildren(attribute,(char*)page+ithRecordOffset);
        //        cout<<"rightChildren="<<rightChildren<<endl;
        setFirstKeyHelper(rightChildren);
    }
    free(page);
}
bool IX_ScanIterator::isKeyInRange(void* key){
    if(IndexManager::instance()->isKeyEqual(attribute,key,lowKey) && lowKeyInclusive)return true;
    if(IndexManager::instance()->isKeyEqual(attribute,key,lowKey) && !lowKeyInclusive)return false;
    if(IndexManager::instance()->isKeyEqual(attribute,key,highKey) && highKeyInclusive)return true;
    if(IndexManager::instance()->isKeyEqual(attribute,key,highKey) && !highKeyInclusive)return false;
    if(IndexManager::instance()->isKeySmaller(attribute,key,highKey) && IndexManager::instance()->isKeySmaller(attribute,lowKey,key))
        return true;
    else
        return false;

}
int IndexManager::getNextLeafPageNum(void* page){
    int nextLeafPageNum;
    memcpy(&nextLeafPageNum,(char*)page+NEXT_LEAF_PAGE_OFFSET,INT_BYTE);
    return nextLeafPageNum;
}
RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{


//       cout<<"curKeyPage="<<curKeyPage<<",curKeySlotNum="<<curKeySlotNum<<endl;

    if(curKeyPage==-1 && curKeySlotNum==-1)return -1;
    void* page=malloc(PAGE_SIZE);
    memset (page,0,PAGE_SIZE);
    ixfileHandle->readPage(curKeyPage,page);
    short slotCount=IndexManager::instance()->getSlotCount(page);
    if(curKeySlotNum>=slotCount)return -1;


    short ithRecordOffset=IndexManager::instance()->getIthRecordOffset(page, curKeySlotNum);
    //    cout<<"ithRecordOffset="<<ithRecordOffset<<endl;
    IndexManager::instance()->extractKey(key,attribute,(char*)page+ithRecordOffset);
    //    cout<<"curKey:";
    //    IndexManager::instance()->printKey(key,attribute);cout<<endl;
    if(!isKeyInRange(key)){
        //        cout<<"how>?"<<endl;
        free(page);
        return -1;
    }
    //cout<<"WW"<<endl;
    short ithRecordLen=IndexManager::instance()->getIthRecordLen(page,curKeySlotNum);

    void* temp=malloc(ithRecordLen);
    memset (temp,0,ithRecordLen);
    RecordBasedFileManager::instance()->setNthNullBitToOne((char*)temp+SHORT_BYTE,3);
    RecordBasedFileManager::instance()->setNthNullBitToOne((char*)temp+SHORT_BYTE,4);

    if(memcmp(temp,(char*)page+ithRecordOffset,ithRecordLen)==0){
        //        cout<<"WW1"<<endl;
        if(curKeySlotNum+1>=slotCount){
            int nextLeafPage=IndexManager::instance()->getNextLeafPageNum(page);
            if(nextLeafPage==-1){
                curKeyPage=-1;
                curKeySlotNum=-1;
            }else{
                curKeyPage=nextLeafPage;
                curKeySlotNum=0;

            }

        }
        else{
            curKeySlotNum+=1;
        }
        free(page);
        free(temp);
        return getNextEntry(rid,key);
    }
    rid.pageNum=IndexManager::instance()->getridPageNum(attribute,(char*)page+ithRecordOffset);
    rid.slotNum=IndexManager::instance()->getridSlotNum(attribute,(char*)page+ithRecordOffset);
    //    cout<<"WW2="<<rid.pageNum<<","<<rid.pageNum<<endl;

    if(curKeySlotNum+1>=slotCount){
        int nextLeafPage=IndexManager::instance()->getNextLeafPageNum(page);
        if(nextLeafPage==-1){
            curKeyPage=-1;
            curKeySlotNum=-1;
        }else{
            curKeyPage=nextLeafPage;
            curKeySlotNum=0;

        }

    }
    else{
        curKeySlotNum+=1;
    }
    free(page);
    free(temp);
    //    cout<<"WW4="<<curKeyPage<<","<<curKeySlotNum<<endl;
    return SUCCESS;
}
void IX_ScanIterator::reset(){
    setFirstKey();
}
RC IX_ScanIterator::close()
{
    free(lowKey);
    free(highKey);
    return SUCCESS;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return myFileHandle.collectCounterValues(readPageCount,writePageCount,appendPageCount);

}

