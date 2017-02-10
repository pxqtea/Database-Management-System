#include "catalog.h"


RelCatalog::RelCatalog(Status &status) :
	HeapFile(RELCATNAME, status)
{
	// nothing should be needed here
}


const Status RelCatalog::getInfo(const string & relation, RelDesc &record)
{
	if (relation.empty())
		return BADCATPARM;

	Status status;
	Record rec;
	RID rid;
	HeapFileScan* hfs; 

	hfs = new HeapFileScan (RELCATNAME, status);

	//scan the file which has relName=relation
	status = hfs->startScan(0, MAXNAME, STRING, relation.c_str(), EQ); 
	if (status == OK)
	{
		status = hfs->scanNext(rid);
		if (status == OK)
		{
			status = hfs->getRecord(rec); 		//get the searched record 
			memcpy(&record, rec.data, rec.length);  //copy the record 
			(RelDesc) record; 
			delete hfs;
			return status; 
		}
		else if(status == FILEEOF) 
		{
			delete hfs;
			return RELNOTFOUND;
		}
	}

	delete hfs;
	return status; 

}


const Status RelCatalog::addInfo(RelDesc & record)
{
	RID rid;
	InsertFileScan*  ifs;
	Status status;
	Record rec; 

	ifs = new InsertFileScan (RELCATNAME, status); 

	if(status == OK)
	{
		rec.data = (void *) &record; 
		rec.length = sizeof(record); 
		ifs->insertRecord (rec, rid);		//insert record into the relcat
	}
	delete ifs; 
	return status; 

}

const Status RelCatalog::removeInfo(const string & relation)
{
	Status status;
	RID rid;
	HeapFileScan*  hfs;
	Record rec; 

	if (relation.empty()) return BADCATPARM;

	hfs = new HeapFileScan (RELCATNAME, status);

	status = hfs->startScan(0, MAXNAME, STRING, relation.c_str(), EQ);
	if (status == OK)
	{
		status = hfs->scanNext(rid);
		if (status == OK)
		{
			status = hfs->getRecord(rec);   //get the rid of the record
			status = hfs->deleteRecord();	//remove the record from relcat
			delete hfs;
			return status;  
		}
		else if(status == FILEEOF)
		{
			delete hfs;
			return RELNOTFOUND;
		}
	}

	delete hfs; 
	return status; 
}


RelCatalog::~RelCatalog()
{
	// nothing should be needed here
}


AttrCatalog::AttrCatalog(Status &status) :
	HeapFile(ATTRCATNAME, status)
{
	// nothing should be needed here
}


const Status AttrCatalog::getInfo(const string & relation, 
		const string & attrName,
		AttrDesc &record)
{

	Status status;
	RID rid;
	Record rec;
	HeapFileScan*  hfs;

	if (relation.empty() || attrName.empty()) return BADCATPARM;


	hfs = new HeapFileScan (ATTRCATNAME, status);

	//scan the file which has relName=relation
	status = hfs->startScan(0, MAXNAME, STRING, relation.c_str(), EQ);
	status = hfs->scanNext(rid);
	if(status == FILEEOF)
	{
		delete hfs;
		return RELNOTFOUND;
	}
	while (status == OK)
	{
		status = hfs->getRecord(rec);  //get the record with relName = relation		
		if(0 == strcmp(((AttrDesc*)(rec.data))->attrName,attrName.c_str())) //compare if attrName = attrName
		{
			memcpy((void *)&record, rec.data, rec.length);		//copy the searched record 
			(AttrDesc) record; 
			delete hfs;
			return OK;
		} 
		else
			status = hfs->scanNext(rid); 
	}
	delete hfs;
	if(status == FILEEOF) return ATTRNOTFOUND; 
	return status; 
}


const Status AttrCatalog::addInfo(AttrDesc & record)
{
	RID rid;
	InsertFileScan*  ifs;
	Status status;
	Record rec;

	ifs = new InsertFileScan (ATTRCATNAME, status);

	rec.data = (void *)&record; 
	rec.length = sizeof(record); 
	ifs->insertRecord(rec, rid);	//add record to attrCat

	delete ifs; 

}


const Status AttrCatalog::removeInfo(const string & relation, 
		const string & attrName)
{
	Status status;
	Record rec;
	RID rid;
	AttrDesc record;
	HeapFileScan*  hfs;

	if (relation.empty() || attrName.empty()) return BADCATPARM;


	hfs = new HeapFileScan (ATTRCATNAME, status);
	status = hfs->startScan(0, MAXNAME, STRING, relation.c_str(), EQ);
	status = hfs->scanNext(rid);
	if(status == FILEEOF)
	{
		delete hfs;
		return RELNOTFOUND;
	}

	while (status == OK)
	{
		status = hfs->getRecord(rec);  //get record with relName = relation
		if(0 == strcmp(((AttrDesc*)rec.data)->attrName,attrName.c_str())) //compare if attrName = attrName
		{
			status = hfs->deleteRecord();		//remove the record
			return OK; 
		}
		else
			status = hfs->scanNext(rid);
	}
	delete hfs;
	if(status == FILEEOF) return ATTRNOTFOUND;
	else return status;

}


const Status AttrCatalog::getRelInfo(const string & relation, 
		int &attrCnt,
		AttrDesc *&attrs)
{
	Status status;
	RID rid;
	Record rec;
	HeapFileScan*  hfs;
	int count=0;

	if (relation.empty()) return BADCATPARM;

	attrCnt = 0; 
	RelDesc rd;
	status = relCat->getInfo(relation,rd); 

	if(status == OK)
	{
		attrCnt = rd.attrCnt; 
		hfs = new HeapFileScan (ATTRCATNAME, status);
		attrs = new AttrDesc [attrCnt];

		//scan the file which has relName=relation
		status = hfs->startScan(0, MAXNAME, STRING, relation.c_str(), EQ);
		while (status == OK)		//get all the record with the required attrName 
		{
			status = hfs->scanNext(rid);
			if (status == OK && count <= attrCnt ) 	//get all the record needed
			{
				status = hfs->getRecord(rec);
				memcpy(&(attrs[count]), rec.data, rec.length);
				count++;
			}

		}	

		delete hfs;

	}
	if ((status == FILEEOF || status == RELNOTFOUND )&& count > 0) return OK;
	else return status;

}


AttrCatalog::~AttrCatalog()
{
	// nothing should be needed here
}

