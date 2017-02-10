#include "catalog.h"


const Status RelCatalog::createRel(const string & relation, 
		const int attrCnt,
		const attrInfo attrList[])
{
	Status status;
	RelDesc rd;
	AttrDesc ad;
	int offset; 

	if (relation.empty() || attrCnt < 1)
		return BADCATPARM;

	if (relation.length() >= sizeof rd.relName)
		return NAMETOOLONG;

	//check if a relation with the same name exist or not
	status = getInfo(relation, rd);
	if (status != OK)
	{
		//check the length of the relation name
		if(sizeof (relation) > MAXNAME) return NAMETOOLONG; 
		//check the sum of length of attributes exceeds page size
		for (int i=0; i< attrCnt; i++)
		{
			//calculate the cumulative size of all attributes
			offset += attrList[i].attrLen;
			//check the length of each attribute
			if((attrList[i].attrLen) >= MAXSTRINGLEN) return ATTRTOOLONG; 
			if(sizeof (attrList[i].attrName) > MAXNAME) return NAMETOOLONG;
		}
		if (offset > PAGESIZE)
			return ATTRTOOLONG;
		else offset = 0;

		//check for duplicate attributes, if the attrList contains any duplicates
		for (int i = 0; i< attrCnt; i++)
		{
			for (int j=i+1; j<attrCnt; j++)
			{
				if (0 == strcmp(attrList[i].attrName, attrList[j].attrName))		
					return DUPLATTR;
			}
		}

		//add the relation into relCat
		strcpy(rd.relName, relation.c_str());
		rd.attrCnt = attrCnt;
		addInfo(rd); 

		//add all the attributes corresponding to the relation into attrCat
		//all the attributes have the same relName
		ad.attrOffset = 0; 
		for (int i=0; i< attrCnt; i++)
		{
			//check for duplicate attributes.
			strcpy(ad.relName, attrList[i].relName);
			strcpy(ad.attrName, attrList[i].attrName);
			ad.attrOffset += offset;
			ad.attrType = (int)attrList[i].attrType;
			ad.attrLen = attrList[i].attrLen;
			attrCat->addInfo(ad);
			offset = attrList[i].attrLen;
		}

		//creat the heapfile for the relation
		createHeapFile(relation); 
		return OK; 
	}
	else 
		return RELEXISTS; 



}

