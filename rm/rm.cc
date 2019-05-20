
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{

    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	if(tableRecordDescriptor.size()==0)createTableRecordDescriptor(tableRecordDescriptor);
	if(columnRecordDescriptor.size()==0)createColumnRecordDescriptor(columnRecordDescriptor);
}

RelationManager::~RelationManager()
{
}


void RelationManager::prepareColumnTuple(
		int fieldCount,
		unsigned char *nullFieldsIndicator,
		const int tableId,
		const string &columnName,
		const int columnType,
		const int columnLength,
		const int columnPosition,
		void *tuple,
		unsigned short& tupleSize
		)
{
	int offset = 0;

	// Null-indicators
	bool nullBit = false;
	int nullFieldsIndicatorActualSize = RecordBasedFileManager::instance()->getActualByteForNullsIndicator(fieldCount);


	// Null-indicator for the fields
	memcpy((char *) tuple + offset, nullFieldsIndicator,
			nullFieldsIndicatorActualSize);
	offset += nullFieldsIndicatorActualSize;

	// Beginning of the actual data
	// Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
	// e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]

	// Is the tableId field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 7);

	if (!nullBit) {
		memcpy((char *) tuple + offset, &tableId, sizeof(int));
		offset += sizeof(int);
	}

	// Is the columnName field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 6);
	if (!nullBit) {
        int columnNameLength=columnName.length();
		memcpy((char *) tuple + offset, &columnNameLength, sizeof(int));
		offset += sizeof(int);
		memcpy((char *) tuple + offset, columnName.c_str(), columnNameLength);
		offset += columnNameLength;
	}


	// Is the columnType field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 5);
	if (!nullBit) {
		memcpy((char *) tuple + offset, &columnType, sizeof(int));
		offset += sizeof(int);
	}
	// Is the columnLength field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 5);
	if (!nullBit) {
		memcpy((char *) tuple + offset, &columnLength, sizeof(int));
		offset += sizeof(int);
	}
	// Is the columnPosition field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 4);
	if (!nullBit) {
		memcpy((char *) tuple + offset, &columnPosition, sizeof(int));
		offset += sizeof(int);
	}

	tupleSize = offset;


}
void RelationManager::prepareTableTuple(
		int fieldCount,
		unsigned char *nullFieldsIndicator,
		const int tableId,
		const string &tableName,
		const string &fileName,
		const int flag,
		void *tuple,
		unsigned short& tupleSize
		)
{
	int offset = 0;

	// Null-indicators
	bool nullBit = false;
	int nullFieldsIndicatorActualSize = RecordBasedFileManager::instance()->getActualByteForNullsIndicator(fieldCount);


	// Null-indicator for the fields
	memcpy((char *) tuple + offset, nullFieldsIndicator,
			nullFieldsIndicatorActualSize);
	offset += nullFieldsIndicatorActualSize;

	// Beginning of the actual data
	// Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
	// e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]

	// Is the id field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 7);

	if (!nullBit) {
		memcpy((char *) tuple + offset, &tableId, sizeof(int));
		offset += sizeof(int);
	}

	// Is the tableName field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 6);
	if (!nullBit) {
        int tableNameLength=tableName.length();
		memcpy((char *) tuple + offset, &tableNameLength, sizeof(int));
		offset += sizeof(int);
		memcpy((char *) tuple + offset, tableName.c_str(), tableNameLength);
		offset += tableNameLength;
	}

	// Is the fileName field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 5);
	if (!nullBit) {
        int fileNameLength=fileName.length();
		memcpy((char *) tuple + offset, &fileNameLength, sizeof(int));
		offset += sizeof(int);
		memcpy((char *) tuple + offset, tableName.c_str(), fileNameLength);
		offset += fileNameLength;
	}

	// Is the flag field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 4);
	if (!nullBit) {
		memcpy((char *) tuple + offset, &flag, sizeof(int));
		offset += sizeof(int);
	}

	tupleSize = offset;

}
void RelationManager::createTableRecordDescriptor(vector<Attribute> &recordDescriptor) {

	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "table-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	recordDescriptor.push_back(attr);

	attr.name = "file-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	recordDescriptor.push_back(attr);

	attr.name = "flag";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4; // 0=user, 1=system
	recordDescriptor.push_back(attr);
}
void RelationManager::createColumnRecordDescriptor(vector<Attribute> &recordDescriptor) {

	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "column-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	recordDescriptor.push_back(attr);

	attr.name = "column-type";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "column-length";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "column-position";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);
}


RC RelationManager::deleteCatalog()
{
	RC deleteTableRC=deleteTable("Tables",SystemManager);
	RC deleteColumnsRC=deleteTable("Columns",SystemManager);
	return deleteTableRC || deleteColumnsRC;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
//    cout<<"creating "<<tableName<<endl;
	if(tableExists(tableName) && tableName!="Tables" && tableName!="Columns")return -1;
	if(tableName!="Tables" && tableName!="Columns")RecordBasedFileManager::instance()->createFile(tableName);
    int id=0;
    if(tableName=="Tables")id=1;
    else if(tableName=="Columns")id=2;
    else{
    	RM_ScanIterator scanTablesAll;
    	vector<string> attributeNames;
    	attributeNames.push_back("table-name");

    	scan("Tables","",NO_OP,NULL,attributeNames,scanTablesAll);

    	void *returnedData = malloc(200);
    	RID rid;

    	while(scanTablesAll.getNextTuple(rid, returnedData) != RM_EOF)
    	{

    		char* temp=(char*)malloc(12);
    		memcpy(temp,(char*)returnedData+5,12);
////    		printTuple(tableRecordDescriptor,returnedData);
    		id++;
    		free(temp);
    	}
    	id=id+1;
    	scanTablesAll.close();
    	free(returnedData);
    }
    unsigned char* tuple=(unsigned char*)malloc(PAGE_SIZE);
    unsigned short tupleSize;
    RID rid;
    //insert to Tables
	FileHandle fileHandle;
	//inside insertTuple function there is a fildHandle, so we do not have to open columns here
	//RecordBasedFileManager::instance()->openFile("Tables",fileHandle);
	int tableNullFieldsIndicatorActualSize = RecordBasedFileManager::instance()->getActualByteForNullsIndicator(tableRecordDescriptor.size());
	unsigned char *tableNullsIndicator = (unsigned char *) malloc(tableNullFieldsIndicatorActualSize);
	memset(tableNullsIndicator, 0, tableNullFieldsIndicatorActualSize);
	prepareTableTuple(tableRecordDescriptor.size(),tableNullsIndicator,id,tableName,tableName,0,tuple,tupleSize);
	insertTuple("Tables", tuple, rid,SystemManager);
    free(tableNullsIndicator);
//cout<<"-----"<<endl;
    //insert to Columns
	for(int i=0;i<attrs.size();i++){
	    int columnNullFieldsIndicatorActualSize = RecordBasedFileManager::instance()->getActualByteForNullsIndicator(columnRecordDescriptor.size());
	    unsigned char *columnNullsIndicator = (unsigned char *) malloc(columnNullFieldsIndicatorActualSize);
		memset(columnNullsIndicator, 0, columnNullFieldsIndicatorActualSize);
		prepareColumnTuple(columnRecordDescriptor.size(),columnNullsIndicator,id,attrs[i].name,	attrs[i].type,attrs[i].length,i+1,tuple,tupleSize);
		insertTuple("Columns", tuple, rid,SystemManager);
        free(columnNullsIndicator);

	}
    free(tuple);


    return 0;
}





RC RelationManager::createCatalog()
{
	RecordBasedFileManager::instance()->createFile("Tables");
	RecordBasedFileManager::instance()->createFile("Columns");

	createTable("Tables",tableRecordDescriptor);
	createTable("Columns",columnRecordDescriptor);

//	RC createTableRBFFile=RecordBasedFileManager::instance()->createFile("Tables");
//	RC createColumnRBFFile=RecordBasedFileManager::instance()->createFile("Columns");
//	if(createTableRBFFile!=0 || createColumnRBFFile!=0)return -1;
//	tableRecordDescriptor.clear();
//	createTableRecordDescriptor(tableRecordDescriptor);
//	columnRecordDescriptor.clear();
//	createColumnRecordDescriptor(columnRecordDescriptor);
//
//	int tableNullFieldsIndicatorActualSize = RecordBasedFileManager::instance()->getActualByteForNullsIndicator(tableRecordDescriptor.size());
//
//    unsigned char *tableNullsIndicator = (unsigned char *) malloc(tableNullFieldsIndicatorActualSize);
//	memset(tableNullsIndicator, 0, tableNullFieldsIndicatorActualSize);
//    unsigned char* tuple=(unsigned char*)malloc(PAGE_SIZE);
//    unsigned short tupleSize;
//    RID rid;
//    prepareTableTuple(tableRecordDescriptor.size(),tableNullsIndicator,1,"Tables","Tables",1,tuple,tupleSize);
//    insertTuple("Tables", tuple, rid);
//    prepareTableTuple(tableRecordDescriptor.size(),tableNullsIndicator,2,"Columns","Columns",2,tuple,tupleSize);
//    insertTuple("Tables", tuple, rid);
//
//    int columnNullFieldsIndicatorActualSize = RecordBasedFileManager::instance()->getActualByteForNullsIndicator(columnRecordDescriptor.size());
//    unsigned char *columnNullsIndicator = (unsigned char *) malloc(columnNullFieldsIndicatorActualSize);
//	memset(columnNullsIndicator, 0, columnNullFieldsIndicatorActualSize);
//
//    prepareColumnTuple(columnRecordDescriptor.size(),columnNullsIndicator,1,"table-id",	TypeInt,4,1,tuple,tupleSize);
//    insertTuple("Columns", tuple, rid);
//    prepareColumnTuple(columnRecordDescriptor.size(),columnNullsIndicator,1,"table-name", TypeVarChar,50,2,tuple,tupleSize);
//    insertTuple("Columns", tuple, rid);
//    prepareColumnTuple(columnRecordDescriptor.size(),columnNullsIndicator,1,"file-name", TypeVarChar,50,3,tuple,tupleSize);
//    insertTuple("Columns", tuple, rid);
//
//    prepareColumnTuple(columnRecordDescriptor.size(),columnNullsIndicator,2,"table-id", TypeInt,4,1,tuple,tupleSize);
//    insertTuple("Columns", tuple, rid);
//    prepareColumnTuple(columnRecordDescriptor.size(),columnNullsIndicator,2,"column-namne", TypeVarChar,50,2,tuple,tupleSize);
//    insertTuple("Columns", tuple, rid);
//    prepareColumnTuple(columnRecordDescriptor.size(),columnNullsIndicator,2,"column-type", TypeInt,4,3,tuple,tupleSize);
//    insertTuple("Columns", tuple, rid);
//    prepareColumnTuple(columnRecordDescriptor.size(),columnNullsIndicator,2,"column-length", TypeInt,4,4,tuple,tupleSize);
//    insertTuple("Columns", tuple, rid);
//    prepareColumnTuple(columnRecordDescriptor.size(),columnNullsIndicator,2,"column-position", TypeInt,4,5,tuple,tupleSize);
//    insertTuple("Columns", tuple, rid);

    return 0;
}
bool RelationManager::tableExists(const string &tableName){
	return RecordBasedFileManager::instance()->fileExists(tableName);
}

RC RelationManager::deleteTable(const string &tableName, Permission p){
	if(p==User && (tableName=="Tables" || tableName=="Columns"))return -1;
	if(p==SystemManager && (tableName=="Tables" || tableName=="Columns")){
		return RecordBasedFileManager::instance()->destroyFile(tableName);
	}
	if(!tableExists(tableName)){
		return -1;
	}else{
    	RM_ScanIterator scanTablesGetID;
    	vector<string> attributeNames;
    	attributeNames.push_back("table-id");
    	unsigned char* value=(unsigned char*)malloc(PAGE_SIZE);
    	unsigned char* tableNameCString=(unsigned char*)malloc(tableName.length());
    	memcpy(tableNameCString,tableName.c_str(),tableName.length());
    	prepareScanValue(value,TypeVarChar,tableNameCString,tableName.length());
     	scan("Tables","table-name",EQ_OP,value,attributeNames,scanTablesGetID);
    	void *returnedData = malloc(4096);
    	RID rid;
    	int count=0;
    	int tableID;
    	while(scanTablesGetID.getNextTuple(rid, returnedData) != RM_EOF)
    	{
    		deleteTuple("Tables", rid,SystemManager);
    		memcpy(&tableID,(unsigned char*)returnedData+1,4);
    		count++;
    	}
    	scanTablesGetID.close();

        RM_ScanIterator scanColumnsGetAttributes;
        scan("Columns","table-id",EQ_OP,&tableID,attributeNames,scanColumnsGetAttributes);
    	while(scanColumnsGetAttributes.getNextTuple(rid, returnedData) != RM_EOF)
    	{
    		deleteTuple("Columns", rid, SystemManager);
    	}
    	free(tableNameCString);
    	free(value);
    	free(returnedData);
    	scanColumnsGetAttributes.close();
		return RecordBasedFileManager::instance()->destroyFile(tableName);
	}
}

RC RelationManager::deleteTable(const string &tableName)
{
	return deleteTable(tableName,User);
}
void RelationManager::prepareScanValue(void* returnValue,AttrType attrType,void* data,int dataSize){
	if(attrType==TypeVarChar){
		int len=dataSize;

		memcpy(returnValue,&len,INT_BYTE);
		memcpy((char*)returnValue+INT_BYTE,data,dataSize);

	}else if(attrType==TypeInt){

		memcpy(returnValue,data,INT_BYTE);
	}else if(attrType==TypeReal){

		memcpy(returnValue,data,FLOAT_BYTE);
	}
	return;
}
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{

	//read tuple, parse it with columns descriptor
	attrs.clear();
    if(tableName=="Tables"){
    	for(Attribute a : tableRecordDescriptor)attrs.push_back(a);
    }else if(tableName=="Columns"){
    	for(Attribute a : columnRecordDescriptor)attrs.push_back(a);
    }else{
    	RM_ScanIterator scanTablesGetID;
    	vector<string> attributeNames;
    	attributeNames.push_back("table-id");
    	unsigned char* value=(unsigned char*)malloc(PAGE_SIZE);
    	unsigned char* tableNameCString=(unsigned char*)malloc(tableName.length());
    	memcpy(tableNameCString,tableName.c_str(),tableName.length());
    	prepareScanValue(value,TypeVarChar,tableNameCString,tableName.length());
     	scan("Tables","table-name",EQ_OP,value,attributeNames,scanTablesGetID);
    	void *returnedData = malloc(4096);
    	RID rid;
    	int count=0;
    	int tableID;
    	while(scanTablesGetID.getNextTuple(rid, returnedData) != RM_EOF)
    	{


    		memcpy(&tableID,(unsigned char*)returnedData+1,4);
//     		cout<<"@@@@@@@@@@@@@@@@return id="<<tableID<<endl;
    		count++;
    	}
    	scanTablesGetID.close();
//        cout<<"-------count="<<count<<endl;
        RM_ScanIterator scanColumnsGetAttributes;
        attributeNames.clear();
        attributeNames.push_back("column-name");
        attributeNames.push_back("column-type");
        attributeNames.push_back("column-length");
        scan("Columns","table-id",EQ_OP,&tableID,attributeNames,scanColumnsGetAttributes);
    	while(scanColumnsGetAttributes.getNextTuple(rid, returnedData) != RM_EOF)
    	{

            int offset=1; // 1 is size of nullBitIndicator of the returnData
    		//get name

            int nameLen;
            memcpy(&nameLen,(char*)returnedData+offset,INT_BYTE);
            offset+=INT_BYTE;
            char* name=(char*)malloc(nameLen);

            memcpy(name,(char*)returnedData+offset,nameLen);
            offset+=nameLen;
    		//get attribute type
            AttrType type;
            memcpy(&type,(char*)returnedData+offset,INT_BYTE);
            offset+=INT_BYTE;
    		//get attribute length
            AttrLength length;
            memcpy(&length,(char*)returnedData+offset,INT_BYTE);
            offset+=INT_BYTE;
            Attribute a;
            for(int j=0;j<nameLen;j++)a.name+=name[j];
            a.type=type;
            a.length=length;
            attrs.push_back(a);
            free(name);
   		    count++;
    	}
    	free(tableNameCString);
        free(value);
        free(returnedData);
    	scanColumnsGetAttributes.close();
    }
    return 0;
}
void RelationManager::convertNthAttributeTeacherData(vector<Attribute>& attrs, int n,const void* teacherData, void* nthAttributeTeacherData,unsigned short& nthAttributeTeacherDataSize){
//    void* nthAttributeTeacherData=malloc(PAGE_SIZE);
//    unsigned short nthAttributeTeacherDataSize;
//    unsigned short myDataSize;
//    void* myData=malloc(PAGE_SIZE);
//    RecordBasedFileManager::instance()->convertData(attrs, teacherData,myDataSize, (unsigned char*)myData, RBF);
//    RecordBasedFileManager::instance()->convertNthAttribute((unsigned char*)myData,(unsigned char*)key,keySize,attributePos,attrs);

	int offset=RecordBasedFileManager::instance()->getActualByteForNullsIndicator(attrs.size());;
	for(int i=0;i<attrs.size();i++){
		unsigned char nthNullBit=RecordBasedFileManager::instance()->getNthNullBit(i,(unsigned char*)teacherData);
		if(i==n){
			if(attrs[i].type==TypeVarChar){
				int len;
			    memcpy(&len,(char*)teacherData+offset,INT_BYTE);
			    memcpy(nthAttributeTeacherData,(char*)teacherData+offset,INT_BYTE+len);
			    nthAttributeTeacherDataSize=INT_BYTE+len;
			}else
				memcpy(nthAttributeTeacherData,(char*)teacherData+offset,INT_BYTE);
			    nthAttributeTeacherDataSize=INT_BYTE;
			break;
		}
		if(nthNullBit==1)continue;
		else{
		  if(attrs[i].type==TypeVarChar){
			  int len;
			  memcpy(&len,(char*)teacherData+offset,INT_BYTE);
			  offset+=INT_BYTE;
			  offset+=len;
		  }else{
			  offset+=INT_BYTE;
		  }
		}
	}
	return;
}
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid){
	RC rc;
	rc= insertTuple(tableName,data,rid,User);
    vector<Attribute> tempAttrs;
    tempAttrs.clear();
	getAttributes(tableName, tempAttrs);
    for(int i=0;i<tempAttrs.size();i++){
    	if(tableExists(tableName+"_"+tempAttrs[i].name+"_idx")){
//    		cout<<"WW"<<endl;
    		 IXFileHandle ixfileHandle;
    		 IndexManager::instance()->openFile(tableName+"_"+tempAttrs[i].name+"_idx",ixfileHandle);

    		 unsigned short tempSize;
    		 void* temp=malloc(PAGE_SIZE);

    		 convertNthAttributeTeacherData(tempAttrs, i, data,
    				 temp, tempSize);
    		 rc=IndexManager::instance()->insertEntry(ixfileHandle, tempAttrs[i], temp, rid);
//    		 cout<<"rid="<<rid.pageNum<<","<<rid.slotNum<<endl;
//
//
//    		 void* testX=malloc(PAGE_SIZE);
//    		 readTuple(tableName, rid, testX);
//    		 printTuple(tempAttrs,testX);
//    		 free(testX);

    		 assert(rc==SUCCESS && "insertEntry should not fail");
    		 free(temp);
//    		 cout<<"WW2"<<endl;
    		 IndexManager::instance()->closeFile(ixfileHandle);
    	}
    }


//	cout<<"rc="<<rc<<endl;
	return rc;
}
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid, Permission p)
{
	if((tableName=="Tables" || tableName=="Columns") && p==User)return -1;
	FileHandle fileHandle;
//	cout<<"tableName="<<tableName<<endl;
	if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle) != 0)
	{
//		cout<<"@@"<<endl;
		return -1;
	}
	vector<Attribute> attrs;
	getAttributes(tableName,attrs);
//cout<<"attrs.size()="<<attrs.size()<<endl;
	RC insertRC=RecordBasedFileManager::instance()->insertRecord(fileHandle, attrs, data, rid);

	fileHandle.closeFile();

    return insertRC;
}
RC RelationManager::deleteTuple(const string &tableName, const RID &rid){
	return deleteTuple(tableName,rid, User);
}
RC RelationManager::deleteTuple(const string &tableName, const RID &rid, Permission p)
{
	if((tableName=="Tables" || tableName=="Columns") && p==User)return -1;
	FileHandle fileHandle;
	if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}
	vector<Attribute> attrs;
	getAttributes(tableName,attrs);
	RC deleteRC=RecordBasedFileManager::instance()->deleteRecord(fileHandle, attrs, rid);

	fileHandle.closeFile();

    return deleteRC;
}
RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid){
	return updateTuple(tableName,data,rid,User);
}
RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid, Permission p)
{
	if((tableName=="Tables" || tableName=="Columns") && p==User)return -1;

	FileHandle fileHandle;
	if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle) != 0)
	{

		return -1;
	}

	vector<Attribute> attrs;
	getAttributes(tableName,attrs);

	RC updateRC=RecordBasedFileManager::instance()->updateRecord(fileHandle, attrs, data,rid);

	fileHandle.closeFile();

    return updateRC;

}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{

	FileHandle fileHandle;
	if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}
	vector<Attribute> attrs;
	getAttributes(tableName,attrs);
	RC readRC=RecordBasedFileManager::instance()->readRecord(fileHandle, attrs, rid,data);
	fileHandle.closeFile();



    return readRC;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return RecordBasedFileManager::instance()->printRecord(attrs,data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fileHandle;
	if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}
	vector<Attribute> attrs;
	getAttributes(tableName,attrs);
	RC readAttrRC=RecordBasedFileManager::instance()->readAttribute(fileHandle, attrs, rid, attributeName, data);
	fileHandle.closeFile();

	return readAttrRC;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	FileHandle fileHandle;
	if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}

	vector<Attribute> attrs;
	getAttributes(tableName,attrs);
	RecordBasedFileManager::instance()->scan(fileHandle,attrs,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfm_ScanIterator);

	return 0;

}
void RM_ScanIterator::reset(){
	rbfm_ScanIterator.reset();
}
RC RM_ScanIterator::close() {
	return rbfm_ScanIterator.close();
};
RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	return rbfm_ScanIterator.getNextRecord(rid,data);

}
// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	RelationManager *rm = RelationManager::instance();
	RC rc;
    string indexFileName=tableName+"_"+attributeName+"_idx";
	rc=IndexManager::instance()->createFile(indexFileName);
    if(rc!=SUCCESS)return -1;
    IXFileHandle ixfileHandle;
    rc = IndexManager::instance()->openFile(indexFileName, ixfileHandle);
    assert(rc == SUCCESS && "indexManager::openFile() should not fail.");
    vector<Attribute> attrs;
    rc = rm->getAttributes(tableName, attrs);
    vector<string> projected_attrs;
//    cout<<"attrs.size()="<<attrs.size()<<endl;
    for(int i=0;i<attrs.size();i++)projected_attrs.push_back(attrs[i].name);

    assert(rc == SUCCESS && "RelationManager::getAttributes() should not fail.");
    RM_ScanIterator rmsi;
    rc = rm->scan(tableName, "", NO_OP, NULL, projected_attrs, rmsi);
    assert(rc == SUCCESS && "RelationManager::scan() should not fail.");
    RID rid;
    void *returnedData = malloc(PAGE_SIZE);
    int attributePos=-2;
    for(int i=0;i<projected_attrs.size();i++){
//    	cout<<"projected_attrs="<<projected_attrs[i]<<endl;
    	if(projected_attrs[i]==attributeName){
    		attributePos=i;
    		break;
    	}
    }

    assert(attributePos>=0 && "should be able to find the attributeName");
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
        void* key=malloc(PAGE_SIZE);
        unsigned short keySize;
        unsigned short myDataSize;
        void* myData=malloc(PAGE_SIZE);
        RecordBasedFileManager::instance()->convertData(attrs, returnedData,myDataSize, (unsigned char*)myData, RBF);
        RecordBasedFileManager::instance()->convertNthAttribute((unsigned char*)myData,(unsigned char*)key,keySize,attributePos,attrs);
        rc=IndexManager::instance()->insertEntry(ixfileHandle, attrs[attributePos], key, rid);
        assert(rc==SUCCESS && "insertEntry should not fail");
        free(key);
        free(myData);

    }
//    cout<<"total pgae="<<ixfileHandle.getNumberOfPages()<<endl;
//    void* page=malloc(PAGE_SIZE);
//    ixfileHandle.readPage(0,page);
//    cout<<"next leafpage="<<IndexManager::instance()->getNextLeafPageNum(page);
//    cout<<"-----"<<endl;
//    IndexManager::instance()->printAllLeafPage(ixfileHandle,attrs[attributePos]);

    free(returnedData);
    rmsi.close();
    rc = IndexManager::instance()->closeFile(ixfileHandle);
    assert(rc == SUCCESS && "indexManager::closeFile() should not fail.");
	return SUCCESS;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	string indexFileName=tableName+"_"+attributeName+"_idx";
	return IndexManager::instance()->destroyFile(indexFileName);
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{

    RC rc;
    rm_IndexScanIterator.ix_ScanIterator;
    rm_IndexScanIterator.ixFileHandle;

    IndexManager* ix=IndexManager::instance();
    string indexFileName=tableName+"_"+attributeName+"_idx";
//    cout<<"in rm.cc:"<<"indexFileName="<<indexFileName<<endl;
    rc=ix->openFile(indexFileName,rm_IndexScanIterator.ixFileHandle);
//    cout<<"indexFileName="<<indexFileName<<endl;
    assert(rc == SUCCESS && "indexManager::openFile() should not fail.");
    vector<Attribute> attrs;
    rc=getAttributes(tableName,attrs);
    assert(rc == SUCCESS && "RelationManager::getAttributes() should not fail.");
    rc= ix->scan(rm_IndexScanIterator.ixFileHandle,
            getAttributeGivenName(attrs,attributeName),
            lowKey,
            highKey,
            lowKeyInclusive,
            highKeyInclusive,rm_IndexScanIterator.ix_ScanIterator);

    assert(rc == SUCCESS && "indexManager::ix->scan() should not fail.");
	return SUCCESS;
}
Attribute RelationManager::getAttributeGivenName(const vector<Attribute> attrs, const string &attributeName){

	for(int i=0;i<attrs.size();i++){
		if(attrs[i].name==attributeName){
			return attrs[i];
		}
	}
	cout<<"attributeName="<<attributeName<<endl;
	for(int i=0;i<attrs.size();i++)cout<<"attrs["<<i<<"]="<<attrs[i].name<<endl;
	assert(false && "should be able to find the attribute given its name");
	Attribute temp;
	return temp;
}
RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
	return ix_ScanIterator.getNextEntry(rid,key);
}
RC RM_IndexScanIterator::close() {
	RC rc;
	rc=ix_ScanIterator.close();
	assert(rc == SUCCESS && "ix_ScanIterator.close() should not fail.");
	rc=IndexManager::instance()->closeFile(ixFileHandle);
	assert(rc == SUCCESS && "IndexManager::instance()->closeFile(ixFileHandle) should not fail.");
	return SUCCESS;
}

