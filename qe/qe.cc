
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {

	itPtr=input;
	conditionPtr=&condition;
    input->getAttributes(attrs);
//    cout<<"attrsSize="<<attrs.size()<<endl;
}
RC Filter::getNextTuple(void *data) {
	RC rc;
	do{

		rc=itPtr->getNextTuple(data);
//		cout<<"rc="<<rc<<endl;
	//	RelationManager::instance()->printTuple(attrs, data);
		if(rc==SUCCESS && matchCondition(attrs, data,*conditionPtr)){
//			cout<<"find 1"<<endl;
			return SUCCESS;
		}
	}while(rc==SUCCESS);


	return rc;
}
void Filter::getAttributes(vector<Attribute> &attrsVector) const{
	attrsVector.clear();
	for(auto a : attrs)attrsVector.push_back(a);
}
//struct Condition {
//    string  lhsAttr;        // left-hand side attribute
//    CompOp  op;             // comparison operator
//    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
//    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
//    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
//};
bool Iterator::matchCondition(vector<Attribute>& attrs,void *techerData, const Condition &condition){
    if(condition.bRhsIsAttr==false){
        // rhs is a value
//    	cout<<"@@@@@@@@@@"<<endl;
    	int leftConditionAttributePos=getAttributePos(attrs, condition.lhsAttr);
        void* nthAttributeTeacherData=malloc(PAGE_SIZE);
        unsigned short nthAttributeTeacherDataSize;
        convertNthAttributeTeacherData(attrs, leftConditionAttributePos,techerData,nthAttributeTeacherData,nthAttributeTeacherDataSize);
        Attribute curAttribute=attrs[leftConditionAttributePos];
        bool ans= RecordBasedFileManager::instance()->
        		compare(curAttribute.type,(unsigned char*)nthAttributeTeacherData,condition.op,(unsigned char*)condition.rhsValue.data,nthAttributeTeacherDataSize);
        return ans;
    }else{
    	assert(false && "I have not implemented it yet");
    	return false;
    }
}
void Iterator::convertNthAttributeTeacherData(vector<Attribute>& attrs, int n,void* teacherData, void* nthAttributeTeacherData,unsigned short& nthAttributeTeacherDataSize){
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
int Iterator::getAttributePos(vector<Attribute>& attrs, string nameOfAttribute){//nameOfAttribute = TableName+"."+AttributeName
	int ans=-1;
//	cout<<"attrNames.size()="<<attrs.size()<<endl;
//	cout<<"nameOfAttribute="<<nameOfAttribute<<endl;
	for(int i=0;i<attrs.size();i++){
////		cout<<"attrNames["<<i<<"]="<<attrNames[i]<<endl;
//		string temp=tableName+"."+attrNames[i];
		if(attrs[i].name==nameOfAttribute){
			return i;
		}
	}
//	if(ans==-1){
//		cout<<"attrNames.size()="<<attrs.size()<<endl;
//		cout<<"nameOfAttribute="<<nameOfAttribute<<endl;
//		for(int i=0;i<attrs.size();i++){
//			cout<<"attrNames["<<i<<"]="<<attrs[i].name<<endl;
//		}
//	}
	assert(ans!=-1 && "should be able to find the pos");
	return -1;
}
Project::Project(Iterator* input, const vector<string> &wantedAttrNames) {
	itPtr=input;
    input->getAttributes(attrs);

    projectedAttrNames=wantedAttrNames;
}
RC Project::getNextTuple(void *data) {
	RC rc;
	int actualByteForNullsIndicator=RecordBasedFileManager::instance()->getActualByteForNullsIndicator(attrs.size());
	do{
		void* temp=malloc(PAGE_SIZE);
		rc=itPtr->getNextTuple(temp);
		if(rc==SUCCESS){
			int offset=0;
			memcpy((char*)data+offset,temp,actualByteForNullsIndicator);
			offset+=actualByteForNullsIndicator;
//			cout<<"projectedAttrNames.size()="<<projectedAttrNames.size()<<endl;
            for(int i=0;i<projectedAttrNames.size();i++){
            	int attributePos=getAttributePos(attrs, projectedAttrNames[i]);
            	void* nthAttributeTeacherData=malloc(PAGE_SIZE);
            	unsigned short nthAttributeTeacherDataSize;
            	convertNthAttributeTeacherData(attrs, attributePos,temp, nthAttributeTeacherData,nthAttributeTeacherDataSize);
            	memcpy((char*)data+offset,nthAttributeTeacherData,nthAttributeTeacherDataSize);
            	offset+=nthAttributeTeacherDataSize;

            }
			return SUCCESS;
		}
	}while(rc==SUCCESS);


	return rc;
}

BNLJoin :: BNLJoin(Iterator *leftIn,            // Iterator of input R
       TableScan *rightIn,           // TableScan Iterator of input S
       const Condition &condition,   // Join condition
       const unsigned numPages       // # of pages that can be loaded into memory,
	                                 //   i.e., memory block size (decided by the optimizer)
){

	leftItPtr=leftIn;
	rightItPtr=rightIn;
	conditionPtr=&condition;
	maxNumPages=numPages;
	leftItPtr->getAttributes(attrs1);
	rightItPtr->getAttributes(attrs2);

//cout<<"woow="<<attrs1.size()<<","<<attrs2.size()<<endl;
};

RC BNLJoin::getNextTuple(void *data) {
//	cout<<" == "<<endl;
//    cout<<"@@attrs1.size()="<<attrs1.size()<<endl;
//    cout<<"@@attrs2.size()="<<attrs2.size()<<endl;

//	RC test;
//	do{
//
//		test=rightItPtr->getNextTuple(data);
////		cout<<"rc="<<rc<<endl;
//		RelationManager::instance()->printTuple(attrs2, data);
//	}while(test==SUCCESS);
//	return test;
//
	RC rc;
	RC rc2;

	void* techearData1=malloc(PAGE_SIZE);
	void* techearData2=malloc(PAGE_SIZE);
	restart1:
	if(!cur1){
       reset();
//       cout<<"@1@attrs1.size()="<<attrs1.size()<<endl;
//       cout<<"@1@attrs2.size()="<<attrs2.size()<<endl;

		rc=leftItPtr->getNextTuple(techearData1);
//		if(maxNumPages==51){
//			cout<<"rc="<<rc<<endl;
//		}
		if(rc!=SUCCESS)return -1;
	}
	//RelationManager::instance()->printTuple(attrs1, techearData1);
	//RelationManager::instance()->printTuple(attrs1, techearData1);
	restart2:

	rc=rightItPtr->getNextTuple(techearData2);
//	if(maxNumPages==51){
//				cout<<"rc2="<<rc<<endl;
//			}
	if(rc!=SUCCESS){
		free(cur1);
		cur1=NULL;
		goto restart1;
	}
	else{

//        if(maxNumPages==51){
//
//        	       cout<<"@1@attrs1.size()="<<attrs1.size()<<endl;
//        	       cout<<"@1@attrs2.size()="<<attrs2.size()<<endl;
//        }
		if(matchCondition2(attrs1,attrs2,techearData1,techearData2,*conditionPtr)){
//			if(maxNumPages==51){
//				cout<<"wow1"<<endl;
//				RelationManager::instance()->printTuple(attrs1,techearData1);
//				cout<<"======="<<endl;
//				RelationManager::instance()->printTuple(attrs2,techearData2);
//				cout<<"wow"<<endl;;
//
//			}
			//combine data here
			combineData(attrs1,attrs2,techearData1,techearData2,data);
//			if(maxNumPages==51){
//				vector<Attribute> temp;
//				getAttributes(temp);
//				RelationManager::instance()->printTuple(temp,data);
//			}
//			vector<Attribute> test;
//			for(auto a:attrs1)test.push_back(a);
//			for(auto a:attrs2)test.push_back(a);
//			RelationManager::instance()->printTuple(test, data);
			return SUCCESS;
		}else{
			goto restart2;
		}
	}

}
void Iterator::combineData(vector<Attribute>& attrs1,vector<Attribute>& attrs2,void *techerData1, void *techerData2, void* data){
	int nullDescriptor1Size=RecordBasedFileManager::instance()->getActualByteForNullsIndicator(attrs1.size());
	int nullDescriptor2Size=RecordBasedFileManager::instance()->getActualByteForNullsIndicator(attrs2.size());
	int nullDescriptorSize=RecordBasedFileManager::instance()->getActualByteForNullsIndicator(attrs1.size()+attrs2.size());
	void* nullDescriptor=malloc(PAGE_SIZE);
	memcpy(nullDescriptor,techerData1,nullDescriptor1Size);
	memcpy(nullDescriptor,techerData1,attrs1.size());
	memcpy((char*)nullDescriptor+attrs1.size(),techerData2,attrs2.size());
	memcpy(data,nullDescriptor,nullDescriptorSize);
	int offset=0;
	memcpy((char*)data+offset,nullDescriptor,nullDescriptorSize);
	offset+=nullDescriptorSize;
	for(int i=0;i<attrs1.size();i++){
        void* nthAttributeTeacherData=malloc(PAGE_SIZE);
        unsigned short nthAttributeTeacherDataSize;
        convertNthAttributeTeacherData(attrs1, i,techerData1,nthAttributeTeacherData,nthAttributeTeacherDataSize);
        memcpy((char*)data+offset,nthAttributeTeacherData,nthAttributeTeacherDataSize);
        offset+=nthAttributeTeacherDataSize;
	}
	for(int i=0;i<attrs2.size();i++){
        void* nthAttributeTeacherData=malloc(PAGE_SIZE);
        unsigned short nthAttributeTeacherDataSize;
        convertNthAttributeTeacherData(attrs2, i,techerData2,nthAttributeTeacherData,nthAttributeTeacherDataSize);
        memcpy((char*)data+offset,nthAttributeTeacherData,nthAttributeTeacherDataSize);
        offset+=nthAttributeTeacherDataSize;
	}
	return;
}


bool Iterator::matchCondition2(vector<Attribute>& attrs1,vector<Attribute>& attrs2,void *techerData1,
		void *techerData2, const Condition &condition){
//	cout<<"@@attrs1="<<attrs1.size()<<",attrs2="<<attrs2.size()<<endl;
//	RelationManager::instance()->printTuple(attrs1, techerData1);
    	int leftConditionAttributePos=getAttributePos(attrs1, condition.lhsAttr);
        void* nthAttributeTeacherData1=malloc(PAGE_SIZE);
        unsigned short nthAttributeTeacherData1Size;
        convertNthAttributeTeacherData(attrs1, leftConditionAttributePos,techerData1,nthAttributeTeacherData1,nthAttributeTeacherData1Size);
//        cout<<"@@attrs1="<<attrs1.size()<<",attrs2="<<attrs2.size()<<endl;
    	int rightConditionAttributePos=getAttributePos(attrs2, condition.rhsAttr);
        void* nthAttributeTeacherData2=malloc(PAGE_SIZE);
        unsigned short nthAttributeTeacherData2Size;
        convertNthAttributeTeacherData(attrs2, rightConditionAttributePos,techerData2,nthAttributeTeacherData2,nthAttributeTeacherData2Size);



        Attribute curAttribute=attrs1[leftConditionAttributePos];
        bool ans= RecordBasedFileManager::instance()->
        		compare(curAttribute.type,(unsigned char*)nthAttributeTeacherData1,condition.op,(unsigned char*)nthAttributeTeacherData2,nthAttributeTeacherData1Size);
//        cout<<"ans="<<ans<<endl;
        return ans;

}
void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	for(auto a:attrs1)attrs.push_back(a);
	for(auto a:attrs2)attrs.push_back(a);
	return;

}

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
       IndexScan *rightIn,          // IndexScan Iterator of input S
       const Condition &condition   // Join condition
){
	leftItPtr=leftIn;
	rightItPtr=rightIn;
	conditionPtr=&condition;
	leftItPtr->getAttributes(attrs1);
	rightItPtr->getAttributes(attrs2);
}
RC INLJoin::getNextTuple(void *data) {
	RC rc;
	RC rc2;

	void* techearData1=malloc(PAGE_SIZE);
	void* techearData2=malloc(PAGE_SIZE);
	restart1:
	if(!cur1){
       reset();

		rc=leftItPtr->getNextTuple(techearData1);
		if(rc!=SUCCESS)return -1;
	}
	//RelationManager::instance()->printTuple(attrs1, techearData1);
	//RelationManager::instance()->printTuple(attrs1, techearData1);
	restart2:
	rc=rightItPtr->getNextTuple(techearData2);
	if(rc!=SUCCESS){
		free(cur1);
		cur1=NULL;
		goto restart1;
	}
	else{
        Attribute a;
        int leftConditionAttributePos=getAttributePos(attrs1, conditionPtr->lhsAttr);
//        bool RecordBasedFileManager::compare(const AttrType attrType,const unsigned char* data1
//        		,const CompOp compOp, const unsigned char* data2,const unsigned short dataSize);
        void* leftKey=malloc(PAGE_SIZE);
        unsigned short leftKeySize;
        convertNthAttributeTeacherData(attrs1, leftConditionAttributePos,
        		techearData1,leftKey,leftKeySize);
//        RelationManager::instance()->printTuple(attrs1, techearData1);
//        IndexManager::instance()->printKey(techearData2,attrs1[leftConditionAttributePos]);
//        cout<<endl;
		if(matchCondition2(attrs1,attrs2,techearData1,
				techearData2, *conditionPtr)){
			combineData(attrs1,attrs2,techearData1,techearData2,data);
			return SUCCESS;
		}else{
			goto restart2;
		}
	}

}
void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	for(auto a:attrs1)attrs.push_back(a);
	for(auto a:attrs2)attrs.push_back(a);
	return;
}
void  INLJoin::reset(){
	rightItPtr->reset();
}
Aggregate::Aggregate(Iterator *input,          // Iterator of input R
          Attribute aggAttr,        // The attribute over which we are computing an aggregate
          AggregateOp op            // Aggregate operation
){
	myOp=op;
	myAggAttr=aggAttr;
	myAttrs.clear();
	input->getAttributes(myAttrs);
	itPtr=input;

}
RC Aggregate::getNextTuple(void *data){
	if(myOp==MIN){
//		cout<<"@@";
		RC rc;
		bool enter=false;
	    void* teacherData=malloc(PAGE_SIZE);
	    void* curMin=NULL;

		while(itPtr->getNextTuple(teacherData)==SUCCESS){
			//RelationManager::instance()->printTuple(myAttrs, teacherData);
			int pos=getAttributePos(myAttrs, myAggAttr.name);
			enter=true;
			void* nthAttributeTeacherData=malloc(PAGE_SIZE);

			unsigned short nthAttributeTeacherDataSize;
			convertNthAttributeTeacherData(myAttrs, pos,teacherData,nthAttributeTeacherData,nthAttributeTeacherDataSize);
			int test;
			memcpy(&test,nthAttributeTeacherData,4);
			if(!curMin){
				curMin=malloc(PAGE_SIZE);
				memcpy(curMin,nthAttributeTeacherData,PAGE_SIZE);
			}else{
				if(RecordBasedFileManager::instance()->compare(myAggAttr.type,(unsigned char*)nthAttributeTeacherData,LT_OP,
						(unsigned char*)curMin,nthAttributeTeacherDataSize)){
					memcpy(curMin,nthAttributeTeacherData,PAGE_SIZE);

				}
			}
		}
        if(enter){
        	int temp;
        	memcpy(&temp,curMin,4);
        	float ans=temp;
        	if(myAggAttr.type==TypeInt){
            	int tempInt;
            	memcpy(&tempInt,curMin,4);
            	float tempFloat=tempInt;
            	memcpy(&ans,&tempFloat,4);
        	}
        	memset(data,0,1);
        	memcpy((char*)data+1,&ans,INT_BYTE);
//        	cout<<"@@";
        	return SUCCESS;
        }
        else return -1;
	}else if(myOp==MAX){
		RC rc;
		bool enter=false;
	    void* teacherData=malloc(PAGE_SIZE);
	    void* curMax=NULL;

		while(itPtr->getNextTuple(teacherData)==SUCCESS){
			//RelationManager::instance()->printTuple(myAttrs, teacherData);
			int pos=getAttributePos(myAttrs, myAggAttr.name);
			enter=true;
			void* nthAttributeTeacherData=malloc(PAGE_SIZE);

			unsigned short nthAttributeTeacherDataSize;
			convertNthAttributeTeacherData(myAttrs, pos,teacherData,nthAttributeTeacherData,nthAttributeTeacherDataSize);
			int test;
			memcpy(&test,nthAttributeTeacherData,4);
			if(!curMax){
				curMax=malloc(PAGE_SIZE);
				memcpy(curMax,nthAttributeTeacherData,PAGE_SIZE);
			}else{
				if(RecordBasedFileManager::instance()->compare(myAggAttr.type,(unsigned char*)nthAttributeTeacherData,GT_OP,
						(unsigned char*)curMax,nthAttributeTeacherDataSize)){
					memcpy(curMax,nthAttributeTeacherData,PAGE_SIZE);

				}
			}
		}
        if(enter){
        	int temp;
        	memcpy(&temp,curMax,4);
        	float ans=temp;
        	if(myAggAttr.type==TypeInt){
            	int tempInt;
            	memcpy(&tempInt,curMax,4);
            	float tempFloat=tempInt;
            	memcpy(&ans,&tempFloat,4);
        	}
        	memset(data,0,1);
        	memcpy((char*)data+1,&ans,INT_BYTE);
//        	cout<<"@@";
        	return SUCCESS;
        }
        else return -1;

	}else if(myOp==AVG){
		RC rc;
		bool enter=false;
	    void* teacherData=malloc(PAGE_SIZE);
        float sum=0;
        int curSize=0;
		while(itPtr->getNextTuple(teacherData)==SUCCESS){
			//RelationManager::instance()->printTuple(myAttrs, teacherData);
			int pos=getAttributePos(myAttrs, myAggAttr.name);
			enter=true;
			void* nthAttributeTeacherData=malloc(PAGE_SIZE);

			unsigned short nthAttributeTeacherDataSize;
			convertNthAttributeTeacherData(myAttrs, pos,teacherData,nthAttributeTeacherData,nthAttributeTeacherDataSize);
            float cur;
            if(myAggAttr.type==TypeInt){
            	int tempInt;
            	memcpy(&tempInt,nthAttributeTeacherData,4);
            	float tempFloat=tempInt;
            	 memcpy(&cur,&tempFloat,4);
            }else
                memcpy(&cur,nthAttributeTeacherData,4);
//            cout<<"cur="<<cur<<endl;
            sum+=cur;
			curSize++;
		}
        if(enter){
        	float ans=sum/curSize;
        	memset(data,0,1);
        	memcpy((char*)data+1,&ans,INT_BYTE);
        	return SUCCESS;
        }
        else return -1;
	}
	else if(myOp==COUNT){
		    float ans;
			RC rc;
			bool enter=false;
		    void* teacherData=malloc(PAGE_SIZE);
		    void* curMax=NULL;

			while(itPtr->getNextTuple(teacherData)==SUCCESS){
				//RelationManager::instance()->printTuple(myAttrs, teacherData);
				ans+=1;
				enter=true;
			}
	        if(enter){

	        	memcpy((char*)data+1,&ans,INT_BYTE);
	        	return SUCCESS;
	        }
	        else return -1;
	}
}

