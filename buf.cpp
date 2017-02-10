#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
	cerr << "At line " << __LINE__ << ":" << endl << "  "; \
	cerr << "This condition should hold: " #c << endl; \
	exit(1); \
} \
}

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
	numBufs = bufs;

	bufTable = new BufDesc[bufs];                //build the bufTable //pxq
	memset(bufTable, 0, bufs * sizeof(BufDesc));
	for (int i = 0; i < bufs; i++) 
	{
		bufTable[i].frameNo = i;
		bufTable[i].valid = false;
	}

	bufPool = new Page[bufs];
	memset(bufPool, 0, bufs * sizeof(Page));

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;     //why times 1.2 //pxq
	hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

	clockHand = bufs - 1;
}

//----------------------------------------
// Destructor of the class BufMgr
//----------------------------------------
//

BufMgr::~BufMgr() {

	//call the function to desctructe the BufDesc table
	for (int i = 0; i < numBufs; i++)  //check the dirty bit 
	{
		BufDesc* tmpBuf = &bufTable[i];
		if (bufTable->valid == true)
		{
			if (bufTable->dirty == true)   //if dirtied, write the page into disk
			{
				tmpBuf->file->writePage(tmpBuf->pageNo, &(bufPool[i]));   //call class File to write the dirty page out
			}
		}
	}
	delete [] bufTable; 
	delete [] bufPool; 

}



//----------------------------------------
// Allocates a free frame using the clock algorithm
//----------------------------------------
//

const Status BufMgr::allocBuf(int & frame) {
	int tmppinCnt = 0;    //store the initial position of the clock hand (before advanced) 
	Status status = UNKNOWN; 

	advanceClock();     //advance the clock hand form the current position (just inserted) to the next position

	while (true)
	{
		if (bufTable[clockHand].valid == true)      //the current frame is in use (not free)
		{
			if (bufTable[clockHand].refbit == true)  //the current frame has been used recently
			{
				bufTable[clockHand].refbit = false; //clear the referbit
				advanceClock();			 // advance the clock hand
			}
			else if (bufTable[clockHand].refbit == false) //the current frame has not been used recently
			{
				if(bufTable[clockHand].pinCnt != 0)   //the current frame is pinned 
				{
					advanceClock();			//
					tmppinCnt++; 
					if(tmppinCnt ==numBufs)		//all pages are pinned
					{
						return BUFFEREXCEEDED;  //all pages are pinned
						break;			// break the while loop
					}
				}
				else if (bufTable[clockHand].pinCnt == 0)  // the current frame is unpinned
				{
					if(bufTable[clockHand].dirty == false)  // the page in the current frame is not modified
					{
						if ((status = hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo)) ==OK) //remove the hash info of the "victim"
						{
							frame = bufTable[clockHand].frameNo;
							(bufTable+clockHand)->Clear();          //clear the bufTable info of the victim
							break;  // break the while loop
						}
					}
					else if(bufTable[clockHand].dirty == true)  //the page in the current frame is modified
					{
						BufDesc* tmpBuf = &bufTable[clockHand];
						if((status = tmpBuf->file->writePage(bufTable[clockHand].pageNo,  &(bufPool[bufTable[clockHand].frameNo]))) != OK) //pxq, flush the page into disk
							return status; 
						if ((status = hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo)) ==OK)  //remove hash of victim
						{
							frame = bufTable[clockHand].frameNo;
							(bufTable+clockHand)->Clear();  //remove bufTable info of victim
							break;
						} 
					}
				}
			}
		}
		else if (bufTable[clockHand].valid == false)   // the current frame is free
		{
			frame = bufTable[clockHand].frameNo;
			break;  // break the while loop
		}
	}


	return  OK; 
}



//----------------------------------------
// Read a page either already in the buffer pool or from the disk
//----------------------------------------
//
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {

	Status status= UNKNOWN;
	int tmpframe= -1; 

	if ((status = hashTable->lookup(file, PageNo, tmpframe)) == OK)  //page found in bufferpool 
	{
		bufTable[tmpframe].refbit = true;                // set refbit  
		bufTable[tmpframe].pinCnt++;                     // increase pinCnt
		page = &bufPool[tmpframe];                // return page
		return OK;

	}
	else if (status == HASHNOTFOUND) // page not found in the bufferpool
	{
		if((status = allocBuf(tmpframe))== OK)              //allocate frame for the new page
		{       
			if((status = file->readPage(PageNo,&bufPool[tmpframe]))==OK)     // read the page from file using methods in class File, the page is in "page"
			{	
				if((status = hashTable->insert(file, PageNo, tmpframe))== OK)
				{
					(bufTable+tmpframe)->Set(file, PageNo);
					page = &bufPool[tmpframe];  
				}
			} 
		} 
	}  

	return status;

}



//----------------------------------------
// unpinpage after the DBMS finished on the page pinned previously 
//----------------------------------------
//
const Status BufMgr::unPinPage(File* file, const int PageNo, 
		const bool dirty) {
	Status status= UNKNOWN;
	int tmpframe = -1; 

	//lookup the page first
	if ((status = hashTable->lookup(file, PageNo, tmpframe)) == OK)  //page in the buffer pool
	{
		if(bufTable[tmpframe].pinCnt == 0)         //pinCnt=0, page is already unpinned
			return PAGENOTPINNED;
		else if (bufTable[tmpframe].pinCnt > 0)		//unpin the page, reduce pinCnt, set dirty bit accordingly
		{
			bufTable[tmpframe].pinCnt--;

			if (dirty == true)
				bufTable[tmpframe].dirty = true;        //set the dirty bit accordingly
			return OK;
		}
	}
	return status;          //else, page is not in the buffer pool 
}



//----------------------------------------
// Allocate memory for the new page and add it into the bufferpool system 
//----------------------------------------
//
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)  {

	Status  status = UNKNOWN; 
	int     tmpframe = -1; 

	if ((status = file->allocatePage(pageNo)) == OK)        // allocate a new page in the file (in disk) 
	{
		if ((status = allocBuf(tmpframe)) == OK)        // allocate frame in bufpool for the new page
		{
			if ( (status = hashTable->insert(file, pageNo, tmpframe)) == OK)	//build hash info of the new page
			{
				(bufTable+tmpframe)->Set(file, pageNo);   //build bufTable info of the new page
				page = &bufPool[tmpframe];                //return the pointer to bufpool of the new page
				return OK;
			}

		}	
	}
	return UNIXERR;
}



//----------------------------------------
// Dispose the page in the file 
//----------------------------------------
//
const Status BufMgr::disposePage(File* file, const int pageNo) {
	Status  status = UNKNOWN;
	int  tmpframe = -1;

	if ( (status = hashTable->lookup(file, pageNo, tmpframe)) == OK)       //search if the page is in the bufpool?
	{
		if ((status = hashTable->remove(file, pageNo)) ==OK)           //remove hash info
		{
			if ((status = file->disposePage(pageNo)) == OK)        //dispose the page in the file (on disk)
				(bufTable+tmpframe)->Clear();                          //remove bufTable info of the page
		}
	}
	return status;

}



//----------------------------------------
// Close file after operations with the DBMS (when all instances of a file have been closed) 
//----------------------------------------
//
const Status BufMgr::flushFile(const File* file) {

	Status status=UNKNOWN; 
	int tmpframe = -1; 
	int tmppage = -1; 

	for (int i = 0; i < numBufs; i++)  //check the dirty bit 
	{

		BufDesc* tmpbuf = &(bufTable[i]); 

		if ( bufTable[i].file == file )      //find the pages belongs to the file
		{

			if (bufTable[i].valid == true)     //the page is valid? 
			{
				if (bufTable[i].pinCnt == 0)     //the page is unpinned?
				{
					if (bufTable[i].dirty == true)   //if dirtied, write the page into disk
					{

						tmpframe = bufTable[i].frameNo;
						tmppage = bufTable[i].pageNo; 

						if ((status = tmpbuf->file->writePage(bufTable[i].pageNo, &(bufPool[bufTable[i].frameNo]))) == OK)   //write page back to file
						{
							bufTable[i].dirty = false;        //refresh the page, already updated in file, so not "dirty" anymore
						}
					}
					hashTable->remove(file, bufTable[i].pageNo); //remove hash info of the page
					tmpbuf->Clear();         //remove bufTable info of the page
				}
				else if (bufTable[i].pinCnt > 0)  //page is pinned
					return PAGEPINNED;  
			}
		}
	}
	return status;

}


void BufMgr::printSelf(void) 
{
	BufDesc* tmpbuf;

	cout << endl << "Print buffer...\n";
	for (int i=0; i<numBufs; i++) {
		tmpbuf = &(bufTable[i]);
		cout << i << "\t" << (char*)(&bufPool[i]) 
			<< "\tpinCnt: " << tmpbuf->pinCnt;
		if(tmpbuf->dirty==true) cout << "\t dirty"; else cout << "\t      ";
		if(tmpbuf->refbit==true) cout << "\t refer"; else cout << "\t      ";    

		if (tmpbuf->valid == true)
			cout << "\tvalid\n";
		cout << endl;
	};
}


