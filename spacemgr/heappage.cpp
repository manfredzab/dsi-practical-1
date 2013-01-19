#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "../include/heappage.h"
#include "../include/heapfile.h"
#include "../include/bufmgr.h"
#include "../include/db.h"

using namespace std;

//------------------------------------------------------------------
// Constructor of HeapPage
//
// Input     : Page ID
// Output    : None
//------------------------------------------------------------------

void HeapPage::Init(PageID pageNo)
{
	nextPage = INVALID_PAGE;
	prevPage = INVALID_PAGE;
	numOfSlots = 0;
	pid = pageNo;
	fillPtr = 0;
	freeSpace = HEAPPAGE_DATA_SIZE;
}

void HeapPage::SetNextPage(PageID pageNo)
{
	nextPage = pageNo;
}

void HeapPage::SetPrevPage(PageID pageNo)
{
	prevPage = pageNo;
}

PageID HeapPage::GetNextPage()
{
	return nextPage;
}

PageID HeapPage::GetPrevPage()
{
	return prevPage;
}


//------------------------------------------------------------------
// HeapPage::InsertRecord
//
// Input     : Pointer to the record and the record's length 
// Output    : Record ID of the record inserted.
// Purpose   : Insert a record into the page
// Return    : OK if everything went OK, DONE if sufficient space 
//             does not exist
//------------------------------------------------------------------

Status HeapPage::InsertRecord(char *recPtr, int length, RecordID& rid)
{
	// Check if enough memory is available
	short i;
	for (i = 0; i < numOfSlots; i++)
	{
		// See if there is a space in the slot directory
		if (SLOT_IS_EMPTY(slots[i]))
		{
			break;
		}
	}

	bool emptySlotFound = i < numOfSlots;

	// Calculate the required amount of memory
	int memoryRequired = length + (emptySlotFound ? 0 : sizeof(Slot));

	if (freeSpace < memoryRequired)
	{
		return DONE;
	}

	// Enough memory is available, update record structure
	rid.pageNo = pid;
	rid.slotNo = i;

	// Update slot directory
	SLOT_FILL(slots[i], fillPtr, length);

	// Store the record
	void* destination = data + HEAPPAGE_DATA_SIZE - fillPtr - length;
	memcpy(destination, recPtr, length);

	// Update the remaining memory, offset from the start of data area and the number of available slots
	freeSpace -= memoryRequired;
	fillPtr += length;
	numOfSlots += emptySlotFound ? 0 : 1;

	return OK;
}


//------------------------------------------------------------------
// HeapPage::DeleteRecord 
//
// Input    : Record ID
// Output   : None
// Purpose  : Delete a record from the page
// Return   : OK if successful, FAIL otherwise  
//------------------------------------------------------------------ 

Status HeapPage::DeleteRecord(const RecordID& rid)
{
	if (rid.pageNo != pid ||
		rid.slotNo >= numOfSlots ||
		rid.slotNo < 0 ||
		SLOT_IS_EMPTY(slots[rid.slotNo]))
	{
		return FAIL;
	}

	// No compacting
	// freeSpace += slots[rid.slotNo].length;

	// Release memory if the last slot
	if (numOfSlots - 1 == rid.slotNo)
	{
		freeSpace += slots[rid.slotNo].length;
		fillPtr = slots[rid.slotNo].offset;
		numOfSlots--;
	}

	SLOT_SET_EMPTY(slots[rid.slotNo]);

	return OK;
}


//------------------------------------------------------------------
// HeapPage::FirstRecord
//
// Input    : None
// Output   : record id of the first record on a page
// Purpose  : To find the first record on a page
// Return   : OK if successful, DONE otherwise
//------------------------------------------------------------------

Status HeapPage::FirstRecord(RecordID& rid)
{
	short i;
	for (i = 0; i < numOfSlots; i++)
	{
		// Check for a non-empty slot
		if (!SLOT_IS_EMPTY(slots[i]))
		{
			break;
		}
	}

	if (i == numOfSlots)
	{
		return DONE;
	}


	rid.pageNo = pid;
	rid.slotNo = i;

	return OK;
}


//------------------------------------------------------------------
// HeapPage::NextRecord
//
// Input    : ID of the current record
// Output   : ID of the next record
// Return   : Return DONE if no more records exist on the page; 
//            otherwise OK
//------------------------------------------------------------------

Status HeapPage::NextRecord (RecordID curRid, RecordID& nextRid)
{
	if (curRid.pageNo != pid ||
		curRid.slotNo >= numOfSlots ||
		curRid.slotNo < 0 ||
		SLOT_IS_EMPTY(slots[curRid.slotNo]))
	{
		return FAIL;
	}

	short i;
	for (i = curRid.slotNo + 1; i < numOfSlots; i++)
	{
		// Check for a non-empty slot
		if (!SLOT_IS_EMPTY(slots[i]))
		{
			break;
		}
	}

	if (i == numOfSlots)
	{
		return DONE;
	}

	nextRid.pageNo = pid;
	nextRid.slotNo = i;

	return OK;
}


//------------------------------------------------------------------
// HeapPage::GetRecord
//
// Input    : Record ID
// Output   : Records length and a copy of the record itself
// Purpose  : To retrieve a _copy_ of a record with ID rid from a page
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------

Status HeapPage::GetRecord(RecordID rid, char *recPtr, int& length)
{
	if (rid.pageNo != pid ||
		rid.slotNo >= numOfSlots ||
		rid.slotNo < 0 ||
		SLOT_IS_EMPTY(slots[rid.slotNo]))
	{
		return FAIL;
	}

	// Set the length
	length = slots[rid.slotNo].length;

	// Copy over the record
	memcpy(recPtr, data + HEAPPAGE_DATA_SIZE - slots[rid.slotNo].offset - length, length);

    return OK;
}


//------------------------------------------------------------------
// HeapPage::ReturnRecord
//
// Input    : Record ID
// Output   : pointer to the record, record's length
// Purpose  : To output a _pointer_ to the record
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------

Status HeapPage::ReturnRecord(RecordID rid, char*& recPtr, int& length)
{
	if (rid.pageNo != pid ||
		rid.slotNo >= numOfSlots ||
		rid.slotNo < 0 ||
		SLOT_IS_EMPTY(slots[rid.slotNo]))
	{
		return FAIL;
	}

	// Set the length
	length = slots[rid.slotNo].length;

	// Set the pointer to record
	recPtr = data + HEAPPAGE_DATA_SIZE - slots[rid.slotNo].offset - length;

    return OK;
}


//------------------------------------------------------------------
// HeapPage::AvailableSpace
//
// Input    : None
// Output   : None
// Purpose  : To return the amount of available space
// Return   : The amount of available space on the heap file page.
//------------------------------------------------------------------

int HeapPage::AvailableSpace(void)
{
	return freeSpace;
}


//------------------------------------------------------------------
// HeapPage::IsEmpty
// 
// Input    : None
// Output   : None
// Purpose  : Check if there is any record in the page.
// Return   : true if the HeapPage is empty, and false otherwise.
//------------------------------------------------------------------

bool HeapPage::IsEmpty(void)
{
	for (short i = 0; i < numOfSlots; i++)
	{
		if (!SLOT_IS_EMPTY(slots[i]))
		{
			return false;
		}
	}

	return true;
}


void HeapPage::CompactSlotDir()
{
  // Complete this method to get the S+ mark.
  // This method is not required for an S mark.
}

int HeapPage::GetNumOfRecords()
{
	int numOfRecords = 0;

	for (short i = 0; i < numOfSlots; i++)
	{
		if (!SLOT_IS_EMPTY(slots[i]))
		{
			numOfRecords++;
		}
	}

	return numOfRecords;
}
