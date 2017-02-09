/*
 * Transaction.cpp
 *
 *  Created on: Jan 24, 2017
 *      Author: anhmt90
 */

#include "Transaction.hpp"
#include <unistd.h>
#include <atomic>


using namespace std;

void newOrderRandom(Timestamp);

/****************************************************************************/
atomic<uint64_t> GMI_tid{1ull<<63};

unordered_map<uint64_t,Transaction*> TransactionManager;

vector<Transaction*> GarbageTransactions;

uint64_t getTid(){
	return ++GMI_tid;
}

/****************************************************************************/
void Transaction::decreaseCommitDepCounter(){
	//i ? ++CommitDepCounter:--CommitDepCounter;
	--CommitDepCounter;
}
/****************************************************************************/
/*
 * @return:
 * 			1 : V is visible to T, just read V
 * 			0 : V is invisible to T, just ignore V
 * 			-1 : reread V
 */
int Transaction::checkVisibility(Version& V){
	auto T = this;
	// logical read time
	uint64_t RT = T->begin;
	// CHECK VISIBILITY OF VERSIONS READ
	/*
	 * 1st case: Begin and End fields contain timestamps
	 * only versions with End field = INF are visible
	 */

	if((V.begin & 1ull<<63) == 0 && (V.end & 1ull<<63) == 0){
		if(V.begin < RT && RT < V.end){
			// V is visible to the current transaction
			// just read the version
			return 1;
		}
		else
			return 0;
	}


	/*
	 * 2nd case: V's Begin field contains a transaction ID (of TB)
	 */
	else if ((V.begin & 1ull<<63) != 0){
		try{
			auto TB = TransactionManager.at(V.begin);
			auto TS = TB->end;

			if(TB->state == Transaction::State::Active && TS == NOT_SET){
				if(TB->Tid == T->Tid && V.end == INF){
				// for updating a version several times by a same transaction
				// just read V
					return 1;
				}
				else{
					return 0;
				}
			}

			else if(TB->state == Transaction::State::Preparing && TS != NOT_SET){
				if(TS < this->begin && V.end == INF){
					//SPECULATIVELY read V
					++(T->CommitDepCounter);
					TB->CommitDepSet.push_back(T->Tid);
					// just read V
					return 1;
				}
				else
					return 0;
			}

			else if(TB->state == Transaction::State::Committed && TS != NOT_SET){
				if(TS < RT && RT < V.end){
					// just read V
					return 1;
				}
				else
					return 0;
			}

			else if(TB->state == Transaction::State::Aborted){
				// ignore V; it's a garbage version
				// here, we can let the garbage collector point to V
				return 0;
			}

		} catch (out_of_range& oor){
			/*
			 * TB is terminated or not found
			 * reread V's Begin field and try again
			 */
			return -1;
		}
	}


	/*
	 * 3rd case: version's End field contains a transaction ID (of TE)
	 */
	else if ((V.end & 1ull<<63) != 0){
		try{
			auto TE = TransactionManager.at(V.end);
			auto TS = TE->end;
			if(TE->state == Transaction::State::Active && TS == NOT_SET){
				if(TE->Tid == T->Tid && V.end == INF) {
					// just read V
					return 1;
				}
				else
					return 0;
			}
			else if(TE->state == Transaction::State::Preparing && TS != NOT_SET){
				if(TS > RT){
					// V is visible if TE commits,
					// If TE aborts, V will still be visible
					// just read V
					return 1;

				} else if (TS < RT) {
					// if TE commits, V will not be visible
					// if TE aborts, V will be visible
					// SPECULATIVELY ignore V
					++(T->CommitDepCounter);
					TE->CommitDepSet.push_back(T->Tid);
					// just ignore  V
					return 0;
				}
			}
			else if(TE->state == Transaction::State::Committed && TS != NOT_SET){
				if(TS > RT){
					// just read V
					return 1;
				}
			}
			else if(TE->state == Transaction::State::Aborted){
				// just read V
				return 1;
			}

		} catch (out_of_range& oor){
			/*
			 * TE is terminated or not found
			 * reread the End field and try again
			 */
			return -1;
		}
	}

	return 1;

}

/****************************************************************************/
int Transaction::checkUpdatibility(Version& V){
	auto T = this;
	// V.end is a timestamp
	if((V.end & 1ull<<63) == 0) {
		if(V.end == INF)
			return 1;
		else
			return 0;
	}
	// V.end contains a Tid
	else {
		try{
			auto TE = TransactionManager.at(V.end);
			if(TE->state == Transaction::State::Aborted)
				return 1;
//			else if (TE->state == Transaction::State::Preparing && TE->end != NOT_SET){
//				/*
//				 * SPECULATIVE UPDATE
//				 * that is an uncommitted version can be updated
//				 * but the transaction that created it must have completed normal processing.
//				 *
//				 * When transaction T reaches the end of normal processing, it precommits and
//				 * begins its preparation phase. Precommit simply consists of acquiring the
//				 * transaction’s end timestamp and setting the transaction state to Preparing.
//				 *
//				 */
//				return 1;
//			}
			else
				return 0;
		} catch (out_of_range& oor){
			/*
			 * TE is terminated or not found
			 */
			return -1;
		}
	}
}

/****************************************************************************/
Version* Transaction::read(string tableName, Predicate pred, bool from_update){
	if(tableName == "warehouse"){
		auto pkey = pred.pk_int;
		//register the scan into ScanSet
		ScanSet_Warehouse.push_back(make_pair(&warehouse.pk_index, pkey));
		auto range = warehouse.pk_index.equal_range(pred.pk_int);

		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			// the scanned version
//			Warehouse_Tuple V = new Warehouse_Tuple(this->Tid);
			Warehouse::Tuple* V = &(it->second);
			//check predicate
			if(V->w_id != pkey)
				continue;
			else {
				int res = checkVisibility(*V);
				while(res == -1){
					res = checkVisibility(*V);
				}
				if(res == 1){
					// adding V to ReadSet
					if(!from_update)
						this->ReadSet.push_back(V);
					return V;
				}
				else
					continue;
			}
		}
	}
	else if(tableName == "orderline"){
		auto pkey = pred.pk_4int;
		ScanSet_OrderLine.push_back(make_pair(&orderline.pk_index, pkey));
		auto range = orderline.pk_index.equal_range(pred.pk_4int);

		for(auto it = range.first; it != range.second; ++it){
			// the scanned version
			OrderLine::Tuple* V = &(it->second);
			//check predicate
			if(make_tuple(V->ol_o_id, V->ol_d_id, V->ol_w_id, V->ol_number) != pkey)
				continue;
			else {
				int res = checkVisibility(*V);
				while(res == -1){
					res = checkVisibility(*V);
				}
				if(res == 1){
					// adding V to ReadSet
					this->ReadSet.push_back(V);
					return V;
				}
				else
					continue;
			}


		}
	}
	return nullptr;
}

/****************************************************************************/
/*
 * update district
 * set d_next_o_id=o_id+1
 * where d_w_id=w_id and district.d_id=d_id;
 */
/*
 * @param:
 * 		del : delete (true = delete, false = update)
 */
Version* Transaction::update(string tableName, Predicate pred, bool del){
	Version* V = read(tableName, pred, true);
	if(V){
		auto updatable = checkUpdatibility(*V);

		while(updatable == -1)
			updatable = checkUpdatibility(*V); // recheck

		if(updatable == 0)
			return nullptr;

		// V can be updated
		else if (updatable == 1){
			if( (V->end & 1ull<<63) == 0){
				// set V's End field to Tid
				V->end = this->Tid;

				if(del){
					this->WriteSet.push_back(make_pair(V,nullptr));
					return V;
				}
			}
			else
				// T must abort if there is another Transaction has sneaked into V
				return nullptr;

			// create a new version
			if(tableName == "warehouse"){
				Warehouse::Tuple* VN = new Warehouse::Tuple(NOT_SET);
				*VN = *(dynamic_cast<Warehouse::Tuple*>(V));

				VN->begin = this->Tid;
				VN->end = INF;

				// add the new version into the primary index
				VN = &(warehouse.pk_index.insert(make_pair(VN->w_id, *VN))->second);

				// add to WriteSet
				this->WriteSet.push_back(make_pair(V,VN));
				return VN;

			}

			else if (tableName == "orderline"){

			}

		}
	}
	return V;

}

//Warehouse::Tuple* Warehouse::insert(Warehouse::Tuple t){
//	auto range = pk_index.equal_range(t.w_id);
//
//	//start the scan
//	for(auto it = range.first; it != range.second; ++it){
//		Warehouse::Tuple* V = &(it->second);
//		//check predicate
//		if(V->w_id == t.w_id){
//
//		}
//
//
//		//	++this->size;
//		return &(i->second);
//	}
//}

Version* Transaction::insert(string tableName, Version* VI, Predicate pred){
	//	Version* VN = read(tableName, pred, true);
	if(tableName == "warehouse"){
		auto pkey = pred.pk_int;
		this->ScanSet_Warehouse.push_back(make_pair(&warehouse.pk_index, pkey));
		auto range = warehouse.pk_index.equal_range(pred.pk_int);

		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			// the scanned version
			// Warehouse_Tuple V = new Warehouse_Tuple(this->Tid);
			Warehouse::Tuple* V = &(it->second);
			//check predicate
			if(V->w_id == pkey){
				// check Visibility of the Version have the same predicate
				if(checkVisibility(*V) == 1){
					return nullptr;
				}
			}

		}
		VI = &(warehouse.pk_index.insert(make_pair(pkey, *(dynamic_cast<Warehouse::Tuple*>(VI))))->second);
	}
	else if(tableName == "orderline"){

	}

	if(VI->begin == 0 && VI->end == 0){
		VI->begin = this->Tid;
		VI->end = INF;
		this->WriteSet.push_back(make_pair(nullptr, VI));
		return VI;
	}
	else
		throw;
}

/****************************************************************************/
int Transaction::validate(){
	/*
	 * Validation consists of two steps: checking visibility of the
	 * versions read and checking for phantoms.
	 */

	/*
	 * To check visibility transaction T scans its ReadSet and for
	 * each version read, checks whether the version is still visible
	 * as of the end of the transaction.
	 *
	 * @return: -1 visibility validation failed
	 */

	for(auto v : ReadSet){
		//if(v->end < INF  && v->end < this->end)
		if(checkVisibility(*v) != 1)
		{
			return -1;
		}
	}

	/*
	 * To check for phantoms, T walks its ScanSet and repeats each scan
	 * looking for versions that came into existence during T’s lifetime
	 * and are visible as of the end of the transaction.
	 *
	 * @return: -2 phantom validation failed
	 */
	for(auto& v : ScanSet_Warehouse){
		auto range = v.first->equal_range(v.second);
		warehouse.pk_index.begin();
		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			//check predicate
			if((it->second).begin != this->Tid){
				if((it->second).w_id == v.second
						&& (it->second).begin < INF
						&& (it->second).begin > this->begin
						&& (it->second).end <= INF
						&& (it->second).end > this->end){
					// phantom detected
					return -2;
				}
			}
		}
	}

	return 1;
}
/****************************************************************************/
void Transaction::abort(int code){
	this->state = State::Aborted;
	this->AbortNow = true;
	this->CODE = code;
}

void Transaction::commit(){
	/*
	 * write log records here
	 */
	if(this->state == State::Preparing)
		this->state = State::Committed;
}


void Transaction::precommit(){
	if(this->state == State::Active){
		this->state = State::Preparing;
		this->end = getTimestamp();
	}
}

/****************************************************************************/


const int32_t warehouses=5;

int32_t urand(int32_t min,int32_t max) {
	return (random()%(max-min+1))+min;
}


int32_t urandexcept(int32_t min,int32_t max,int32_t v) {
	if (max<=min)
		return min;
	int32_t r=(random()%(max-min))+min;
	if (r>=v)
		return r+1; else
			return r;
}



int32_t nurand(int32_t A,int32_t x,int32_t y) {
	return ((((random()%A)|(random()%(y-x+1)+x))+42)%(y-x+1))+x;
}


void Transaction::execute(int i){
//	newOrderRandom(0);

	/*
	 * Randomize the input
	 */

//	int32_t w_id=urand(1,warehouses);
//	int32_t d_id=urand(1,10);
//	int32_t c_id=nurand(1023,1,3000);
//	int32_t ol_cnt=urand(5,15);
//
//	int32_t supware[15];
//	int32_t itemid[15];
//	int32_t qty[15];
//	for (int32_t i=0; i<ol_cnt; i++) {
//		if (urand(1,100)>1)
//			supware[i]=w_id; else
//				supware[i]=urandexcept(1,warehouses,w_id);
//		itemid[i]=nurand(8191,1,100000);
//		qty[i]=urand(1,10);
//	}
//	Timestamp datetime = 0;
	/***************************************************************************************************/
	/*
	 * update district
	 * set d_next_o_id=o_id+1
	 * where d_w_id=w_id and district.d_id=d_id;
	 */

	/*
	 * Begin the NORMAL PROCESSING PHASE
	 */
//	Warehouse_Tuple* VN = dynamic_cast<Warehouse_Tuple*>(update("warehouse", Predicate((Integer) 4), false));
	if(i==1){
		Warehouse::Tuple* V = dynamic_cast<Warehouse::Tuple*>(read("warehouse", Predicate(Integer(1)), false));
		this->begin;
		if(!V){
			if(this->CommitDepCounter == 0)
				abort(-9);
			else
				abort(-8);
		}
		else{
			cout << " V is read!";
//			cout << "Transaction " << (this->Tid - (1ull<<63))  << " sleeps for 2s... \n";
//			std::this_thread::sleep_for(std::chrono::seconds(2));
		}
	}

	else if(i==2){
//		cout << "Transaction " << (this->Tid - (1ull<<63))  << " sleeps for 2s... \n";
//		std::this_thread::sleep_for(std::chrono::seconds(2));

		Warehouse::Tuple* V = dynamic_cast<Warehouse::Tuple*>(update("warehouse", Predicate(Integer(1)), true));
		if(!V){
			abort(-10);
		}
		else
			cout << "Transaction " << (this->Tid - (1ull<<63))  << " delete-operation done.\n";
		warehouse.pk_index.begin();
	}
	else if(i==3){
//		Warehouse::Tuple row = *(new Warehouse::Tuple(0));
//		row.w_id = 6;
//		row.w_name = row.w_name.castString(string("anhmt").c_str(), string("anhmt").length());
//		row.w_street_1 = row.w_street_1.castString(string("a").c_str(), string("a").length());
//		row.w_street_2 = row.w_street_2.castString(string("b").c_str(), string("b").length());
//		row.w_city = row.w_city.castString(string("c").c_str(), string("c").length());
//		row.w_state = row.w_state.castString(string("d").c_str(), string("d").length());
//		row.w_zip = row.w_zip.castString(string("e").c_str(), string("e").length());
//		row.w_tax = Numeric<4, 4>(1);
//		row.w_ytd = Numeric<12, 2>(1);
//
//		auto V = dynamic_cast<Warehouse::Tuple*>(insert("warehouse", row,  Predicate(Integer(row.w_id))));
//		if(!V){
//			cout<<"cannot insert!\n";
//			throw;
//		}
//		cout << "Transaction " << (Tid - (1ull<<63))  << " sleeps for 2s... \n";
//		std::this_thread::sleep_for(std::chrono::seconds(2));
//		warehouse.pk_index.begin();


		Warehouse::Tuple* row = new Warehouse::Tuple(0);
		row->w_id = 6;
		row->w_name = row->w_name.castString(string("anhmt").c_str(), string("anhmt").length());
		row->w_street_1 = row->w_street_1.castString(string("a").c_str(), string("a").length());
		row->w_street_2 = row->w_street_2.castString(string("b").c_str(), string("b").length());
		row->w_city = row->w_city.castString(string("c").c_str(), string("c").length());
		row->w_state = row->w_state.castString(string("d").c_str(), string("d").length());
		row->w_zip = row->w_zip.castString(string("e").c_str(), string("e").length());
		row->w_tax = Numeric<4, 4>(1);
		row->w_ytd = Numeric<12, 2>(1);

		auto V = dynamic_cast<Warehouse::Tuple*>(insert("warehouse", row,  Predicate(Integer(row->w_id))));
		if(!V){
			cout<<"cannot insert!\n";
			throw;
		}
//		cout << "Transaction " << (Tid - (1ull<<63))  << " sleeps for 5s... \n";
//		std::this_thread::sleep_for(std::chrono::seconds(5));
		warehouse.pk_index.begin();

	}



	else if(i==4){
		cout << "Transaction " << (Tid - (1ull<<63))  << " sleeps for 2s... \n";
		std::this_thread::sleep_for(std::chrono::seconds(2));
		Warehouse::Tuple* row = new Warehouse::Tuple(0);
		row->w_id = 7;
		row->w_name = row->w_name.castString(string("anhmt").c_str(), string("anhmt").length());
		row->w_street_1 = row->w_street_1.castString(string("a").c_str(), string("a").length());
		row->w_street_2 = row->w_street_2.castString(string("b").c_str(), string("b").length());
		row->w_city = row->w_city.castString(string("c").c_str(), string("c").length());
		row->w_state = row->w_state.castString(string("d").c_str(), string("d").length());
		row->w_zip = row->w_zip.castString(string("e").c_str(), string("e").length());
		row->w_tax = Numeric<4, 4>(1);
		row->w_ytd = Numeric<12, 2>(1);

		auto V = dynamic_cast<Warehouse::Tuple*>(insert("warehouse", row,  Predicate(Integer(row->w_id))));
		if(!V){
			cout<<"cannot insert!\n";
			throw;
		}
		cout << "Transaction " << (Tid - (1ull<<63))  << " done.\n";
		warehouse.pk_index.begin();
	}

//	else{
//		VN->w_street_1 = Varchar<20>::castString(string("changed2something").c_str(), string("changed2something").length());
//		warehouse.pk_index.begin();
//	}

	/***************************************************************************************************/
	/*
	 * End of NORMAL PROCESSING PHASE
	 */
	if(this->state != State::Aborted)
		precommit();

	/*
	 * Begin of PREPARATION PHASE
	 */

	//Testing cascaded abort and commit dependencies
	if((this->Tid - (1ull<<63)) == 1){
		cout << "Transaction " << (this->Tid - (1ull<<63))  << " sleeps for 300s... \n";
		std::this_thread::sleep_for(std::chrono::seconds(300));
//		abort(-100);
	}

	/*
	 * VALIDATION
	 */
	int valid = 0;
	if(this->state != State::Aborted){
		valid = validate();
		if( valid > 0 && this->state != State::Aborted){
			// the Transaction must wait here for commit dependencies if there are any
			while(CommitDepCounter!=0 && !(this->AbortNow)){
				cout << "Transaction " << (this->Tid - (1ull<<63))  << " is waiting for CommitDep... \n";
				std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			}
			if(!this->AbortNow && CommitDepCounter == 0){
				commit();
			}
		}
		else
			abort(valid);
	}
	/*
	 * POSTPROCESSING PHASE
	 *
	 * During this phase a committed transaction TC propagates its end
	 * timestamp to the Begin and End fields of new and old versions,
	 * respectively, listed in its WriteSet. An aborted transaction TA sets
	 * the Begin field of its new versions to infinity, thereby making
	 * them invisible to all transactions, and attempts to reset the End
	 * fields of its old versions to infinity.
	 */
	if(this->state == State::Committed){
		for(auto& pair : this->WriteSet){
			// for the old version
			if(pair.first){
				pair.first->end = this->end;
				pair.first->isGarbage = true;
			}
			// for the new version
			if(pair.second){
				pair.second->begin = this->end;
			}
			// for delete operation
			else{
				continue;
			}
		}
		// handle commit dependencies
		for(auto& t : CommitDepSet){
			try{
				auto TD = TransactionManager.at(t);
				TD->decreaseCommitDepCounter();
				if(TD->CommitDepCounter == 0){
					//wake TD up
				}

			} catch(out_of_range& oor){
				// the dependent Transaction does not exist or is terminated
				continue;
			}
		}

		this->CODE = 1;
	}


	else if(this->state == State::Aborted){
		for(auto& pair : this->WriteSet){
			// for the old version
			/*
			 * another transaction may already have detected the abort,
			 * created another new version and reset the End field of the
			 * old version. If so, TA leaves the End field value unchanged.
			 */
			if(pair.first && pair.first->end == this->Tid){
				pair.first->end = INF;
			}
			// for the new version
			if(pair.second){
				pair.second->begin = INF;
				pair.second->isGarbage = true;
			}
			//for delete operation
			else {
				continue;
			}
		}

		// all commit dependent Transactions also have to abort
		for(auto& t : CommitDepSet){
			try{
				auto TD = TransactionManager.at(t);
				// -19: cascaded abort code
				TD->abort(-19);
			} catch(out_of_range& oor){
				// the dependent Transaction does not exist or is terminated
				continue;
			}
		}
	}

//	switch(CODE){
//	case -1:
//		cout << "Transaction "<< this->Tid << " aborted ";
//	}
	else
		throw;

	cout << "Tid = " << (this->Tid - (1ull<<63)) << ", Code = " << (this->CODE) << ", begin = " << begin << ", end = " << end << "\n";

	warehouse.pk_index.begin();
	GarbageTransactions.push_back(this);
	TransactionManager.erase(this->Tid);
}










/*****************************************************************************************************************/

void newOrderRandom(Timestamp now) {
	int32_t w_id=urand(1,warehouses);
	int32_t d_id=urand(1,10);
	int32_t c_id=nurand(1023,1,3000);
	int32_t ol_cnt=urand(5,15);

	int32_t supware[15];
	int32_t itemid[15];
	int32_t qty[15];
	for (int32_t i=0; i<ol_cnt; i++) {
		if (urand(1,100)>1)
			supware[i]=w_id; else
				supware[i]=urandexcept(1,warehouses,w_id);
		itemid[i]=nurand(8191,1,100000);
		qty[i]=urand(1,10);
	}

//	newOrder(w_id,d_id,c_id,ol_cnt,supware,itemid,qty,now);
}
