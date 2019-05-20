#ifndef _qe_h_
#define _qe_h_

#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
    Condition(){};
    Condition(const Condition& temp){
    	lhsAttr=temp.lhsAttr;
    	op=temp.op;
    	bRhsIsAttr=temp.bRhsIsAttr;
    	rhsAttr=temp.rhsAttr;
    	rhsValue.type=temp.rhsValue.type;
    	if(rhsValue.type==TypeVarChar){
    		int len;
    		memcpy(&len,temp.rhsValue.data,INT_BYTE);
    		memcpy(rhsValue.data,temp.rhsValue.data,INT_BYTE+len);
    	}else{
    		memcpy(rhsValue.data,temp.rhsValue.data,INT_BYTE);
    	}
    }
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual void reset()=0;
        virtual ~Iterator() {};
        bool matchCondition(vector<Attribute>& attrs, void *data, const Condition &condition);
        int getAttributePos(vector<Attribute>& attrs, string nameOfAttribute);
        void convertNthAttributeTeacherData(vector<Attribute>& attrs, int n,void* teacherData, void* nthAttributeTeacherData,unsigned short& nthAttributeTeacherDataSize);
        void combineData(vector<Attribute>& attrs1,vector<Attribute>& attrs2,void *techerData1, void *techerData2, void* data);
        bool matchCondition2(vector<Attribute>& attrs1,vector<Attribute>& attrs2,void *techerData1,
        		void *techerData2, const Condition &condition);

};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
		string tableName;
		vector<Attribute> attrs;
		vector<string> attrNames;
        RelationManager &rm;
        RM_ScanIterator *iter;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);
//            cout<<"attrs.size()="<<attrs.size()<<endl;
            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
//            cout<<"rid="<<rid.pageNum<<","<<rid.slotNum<<endl;
        	RC temp=iter->getNextTuple(rid, data);

        	return temp;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };
        void reset(){
        	iter->reset();
        }
        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
		string tableName;
		vector<Attribute> attrs;
		vector<string> attrNames;
        char key[PAGE_SIZE];
        string attrName;
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }
            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };
        void reset(){
        	iter->reset();
        }
        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
	    Iterator* itPtr;
	    const Condition* conditionPtr;
		string tableName;
		vector<Attribute> attrs;
		vector<string> attrNames;
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrsVector) const;
        void reset(){
        	itPtr->reset();
        };

};


class Project : public Iterator {
    // Projection operator
    public:
	    Iterator* itPtr;
	    vector<string> projectedAttrNames;
		string tableName;
		vector<Attribute> attrs;
		vector<string> attrNames;
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &wantedAttrNames);   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &v) const{
        	v.clear();
        	for(string name : projectedAttrNames){
        		for(int i=0;i<attrs.size();i++){
        			if(attrs[i].name==name){
        				v.push_back(attrs[i]);
        				break;
        			}
        		}
        	}
        };
        void reset(){
        	itPtr->reset();
        };
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
		Iterator* leftItPtr;
		Iterator* rightItPtr;
		int maxNumPages;
        void* cur1=NULL;

    	vector<Attribute> attrs1;
    	vector<Attribute> attrs2;

		map<int,void*> intMap;
		map<int,void*>::iterator intMapIt;

		map<float,void*> floatMap;
		map<float,void*>::iterator floatMapIt;

		map<string,void*> stringMap;
		map<string,void*>::iterator stringMapIt;

		const Condition* conditionPtr;
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        );
        ~BNLJoin(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
        void reset(){
        	rightItPtr->reset();
        };
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
		Iterator* leftItPtr;
		IndexScan* rightItPtr;
        void* cur1=NULL;
    	vector<Attribute> attrs1;
    	vector<Attribute> attrs2;
		const Condition* conditionPtr;

        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
        void reset();
};


// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      ){};
      ~GHJoin(){};

      RC getNextTuple(void *data){return QE_EOF;};
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const{};
      void reset(){};
};

class Aggregate : public Iterator {
	vector<Attribute> myAttrs;
	AggregateOp myOp;
	Attribute myAggAttr;
	Iterator* itPtr;
    // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        ){
        	itPtr=input;
        	myOp=op;
        };
        ~Aggregate(){};

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const{
        	attrs.clear();
        	for(auto a : myAttrs)attrs.push_back(a);
        };
        void reset(){};
};

#endif
