#include "catalog.h"

//
// Destroys a relation. It performs the following steps:
//
// 	removes the catalog entry for the relation
// 	destroys the heap file containing the tuples in the relation
//
// Returns:
// 	OK on success
// 	error code otherwise
//

const Status RelCatalog::destroyRel(const string & relation)
{
	Status status;
	RelDesc rd;

	if (relation.empty() || 
			relation == string(RELCATNAME) || 
			relation == string(ATTRCATNAME))
		return BADCATPARM;

	//check whether the relation exist or not
	status = relCat->getInfo(relation, rd);
	if (status == OK)		//destroy the existing relation
	{
		attrCat->dropRelation(relation);	//delete all the attribute info related to this relation in the attrCat
		relCat->removeInfo(relation);           //delete all the info of this relation in the relCat
		destroyHeapFile(relation);  		//destroy the heapfile of the relation
		return OK; 
	}
	else  return status; 

}


//
// Drops a relation. It performs the following steps:
//
// 	removes the catalog entries for the relation
//
// Returns:
// 	OK on success
// 	error code otherwise
//

const Status AttrCatalog::dropRelation(const string & relation)
{
	Status status;
	AttrDesc *attrs;
	int attrCnt=0, i;

	if (relation.empty()) return BADCATPARM;

	status = attrCat->getRelInfo(relation,attrCnt, attrs);   //get the relation info from attrCat
	if (status == OK)
	{
		for (i =0; i< attrCnt; i++)
		{
			status = attrCat->removeInfo(attrs[i].relName,attrs[i].attrName);  //remove the attributes of the relation
		}
		delete []attrs;
	}

	return status;  
}


