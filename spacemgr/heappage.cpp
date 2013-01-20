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
	this->nextPage = INVALID_PAGE;
	this->prevPage = INVALID_PAGE;
	this->numOfSlots = 0;
	this->pid = pageNo;
	this->fillPtr = 0;
	this->freeSpace = HEAPPAGE_DATA_SIZE;
}

void HeapPage::SetNextPage(PageID pageNo)
{
	this->nextPage = pageNo;
}

void HeapPage::SetPrevPage(PageID pageNo)
{
	this->prevPage = pageNo;
}

PageID HeapPage::GetNextPage()
{
	return this->nextPage;
}

PageID HeapPage::GetPrevPage()
{
	return this->prevPage;
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
	for (i = 0; i < this->numOfSlots; i++)
	{
		// See if there is a space in the slot directory
		if (SLOT_IS_EMPTY(this->slots[i]))
		{
			break;
		}
	}

	bool emptySlotFound = i < this->numOfSlots;

	// Calculate the required amount of memory
	int memoryRequired = length + (emptySlotFound ? 0 : sizeof(Slot));

	if (this->freeSpace < memoryRequired)
	{
		return DONE;
	}

	// Enough memory is available, update record structure
	rid.pageNo = this->pid;
	rid.slotNo = i;

	// Update slot directory
	SLOT_FILL(this->slots[i], this->fillPtr, length);

	// Store the record
	char* destination = this->data + HEAPPAGE_DATA_SIZE - this->fillPtr - length;
	memcpy(destination, recPtr, length);

	// Update the remaining memory, offset from the start of data area and the number of available slots
	this->freeSpace -= memoryRequired;
	this->fillPtr += length;
	this->numOfSlots += emptySlotFound ? 0 : 1;

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
	if (rid.pageNo != this->pid ||
		rid.slotNo >= this->numOfSlots ||
		rid.slotNo < 0 ||
		SLOT_IS_EMPTY(this->slots[rid.slotNo]))
	{
		return FAIL;
	}

	for (short i = rid.slotNo + 1; i < this->numOfSlots; i++)
	{
		if (!SLOT_IS_EMPTY(this->slots[i]))
		{
			// Move the record's data
			char* source = this->data + HEAPPAGE_DATA_SIZE - this->slots[i].offset - this->slots[i].length;
			char* destination = source + this->slots[rid.slotNo].length;
			memmove(destination, source, this->slots[i].length);

			// Update the offset of the record that was moved
			this->slots[i].offset -= this->slots[rid.slotNo].length;
		}
	}

	this->freeSpace += this->slots[rid.slotNo].length;
	this->fillPtr -= this->slots[rid.slotNo].length;

	// Release the entry in the slot directory if the last slot
	if (this->numOfSlots - 1 == rid.slotNo)
	{
		this->numOfSlots--;
		this->freeSpace += sizeof(Slot);
	}

	// Empty the slot directory entry
	SLOT_SET_EMPTY(this->slots[rid.slotNo]);

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
	for (i = 0; i < this->numOfSlots; i++)
	{
		// Check for a non-empty slot
		if (!SLOT_IS_EMPTY(this->slots[i]))
		{
			break;
		}
	}

	if (i == this->numOfSlots)
	{
		return DONE;
	}


	rid.pageNo = this->pid;
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
	if (curRid.pageNo != this->pid ||
		curRid.slotNo >= this->numOfSlots ||
		curRid.slotNo < 0 ||
		SLOT_IS_EMPTY(this->slots[curRid.slotNo]))
	{
		return FAIL;
	}

	short i;
	for (i = curRid.slotNo + 1; i < this->numOfSlots; i++)
	{
		// Check for a non-empty slot
		if (!SLOT_IS_EMPTY(this->slots[i]))
		{
			break;
		}
	}

	if (i == this->numOfSlots)
	{
		return DONE;
	}

	nextRid.pageNo = this->pid;
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
	if (rid.pageNo != this->pid ||
		rid.slotNo >= this->numOfSlots ||
		rid.slotNo < 0 ||
		SLOT_IS_EMPTY(this->slots[rid.slotNo]))
	{
		return FAIL;
	}

	// Set the length
	length = this->slots[rid.slotNo].length;

	// Copy over the record
	memcpy(recPtr, this->data + HEAPPAGE_DATA_SIZE - this->slots[rid.slotNo].offset - length, length);

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
	if (rid.pageNo != this->pid ||
		rid.slotNo >= this->numOfSlots ||
		rid.slotNo < 0 ||
		SLOT_IS_EMPTY(this->slots[rid.slotNo]))
	{
		return FAIL;
	}

	// Set the length
	length = this->slots[rid.slotNo].length;

	// Set the pointer to record
	recPtr = this->data + HEAPPAGE_DATA_SIZE - this->slots[rid.slotNo].offset - length;

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
	// Check if there are empty slots in the slot directory
	short i;
	for (i = 0; i < this->numOfSlots; i++)
	{
		// See if there is a space in the slot directory
		if (SLOT_IS_EMPTY(this->slots[i]))
		{
			break;
		}
	}

	bool emptySlotFound = i < this->numOfSlots;

	return this->freeSpace - (emptySlotFound ? 0 : sizeof(Slot));
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
	for (short i = 0; i < this->numOfSlots; i++)
	{
		if (!SLOT_IS_EMPTY(this->slots[i]))
		{
			return false;
		}
	}

	return true;
}


void HeapPage::CompactSlotDir()
{
	// Compact the slot directory
	int compactedSlots = 0;
	for (short i = 0; i < this->numOfSlots; i++)
	{
		if (SLOT_IS_EMPTY(this->slots[i]))
		{
			compactedSlots++;
		}
		else
		{
			this->slots[i - compactedSlots] = this->slots[i];
		}
	}

	// Update page header
	this->numOfSlots -= compactedSlots;
	this->freeSpace += compactedSlots * sizeof(Slot);
}

int HeapPage::GetNumOfRecords()
{
	int numOfRecords = 0;

	for (short i = 0; i < this->numOfSlots; i++)
	{
		if (!SLOT_IS_EMPTY(this->slots[i]))
		{
			numOfRecords++;
		}
	}

	return numOfRecords;
}
