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

/****************************************************************************/

// counter to generate the Transactions ID
atomic<uint64_t> GMI_tid{1ull<<63};

uint64_t getTid(){
	return ++GMI_tid;
}

unordered_map<uint64_t,Transaction*> TransactionManager;

vector<Transaction*> GarbageTransactions;


/****************************************************************************/
/*
 * @return:
 * 			1 : V is visible to T, just read V
 * 			0 : V is invisible to T, just ignore V
 * 			-1 : reread V
 */
bool Transaction::checkVisibility(Version& V){
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
			// just read V
			return 1;
		}
		else
			return 0;
	}


	/*
	 * 2nd case: V's Begin field contains a transaction ID (of TB)
	 */
	else if ((V.begin & 1ull<<63) != 0){
		auto itr = TransactionManager.find(V.begin);
		if( itr != TransactionManager.end()){
			if(V.begin == T->Tid && V.end == INF){
				// for updating a version several times by a same transaction OR
				// for visibility validating a version that first was read (in ReadSet) and
				// then was updated by the same transaction.
				// Because the updating set V's Begin field to the Tid
				// just read V
				return 1;
			}
			auto TB = itr->second;
			auto TS = TB->end;

			if(TB->state == Transaction::State::Active && TS == NOT_SET){
				if(TB->Tid == T->Tid && V.end == INF){
				// for updating a version several times by a same transaction
				// just read V
					return 1;
				}
				else
					return 0;
			}
			else if(TB->state == Transaction::State::Preparing && TS != NOT_SET){
				if(TS < begin && V.end == INF){
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
		} else {
			/*
			 * TB is terminated or not found
			 * reread V's Begin field and try again
			 */
			if(V.isGarbage)
				return 0;
			else {
				cout << "Invalid version!\n";
				throw;
			}

		}
	}


	/*
	 * 3rd case: version's End field contains a transaction ID (of TE)
	 */
	else if ((V.end & 1ull<<63) != 0){
		auto itr = TransactionManager.find(V.end);
		if( itr != TransactionManager.end()){
			auto TE = itr->second;
			auto TS = TE->end;
			if(TE->state == Transaction::State::Active && TS == NOT_SET){
				if(TE->Tid == T->Tid) {
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

		} else {
			/*
			 * TE is terminated or not found
			 * reread the End field and try again
			 */
			if(V.isGarbage)
				return 0;
			else {
				cout << "Invalid version!\n";
				throw;
			}
		}
	}
	return 1;
}

/****************************************************************************/
bool Transaction::checkUpdatibility(Version& V){
	// V.end is a timestamp
	if((V.end & 1ull<<63) == 0) {
		if(V.end == INF)
			return 1;
		else
			return 0;
	}
	// V.end contains a Tid
	else {
		auto itr = TransactionManager.find(V.end);
		if( itr != TransactionManager.end()){
			auto TE = itr->second;
			if(TE->state == Transaction::State::Aborted)
				return 1;
			else if (TE->state == Transaction::State::Preparing && TE->end != NOT_SET){
				/*
				 * SPECULATIVE UPDATE
				 * that is an uncommitted version can be updated
				 * but the transaction that created it must have completed normal processing.
				 *
				 * When transaction T reaches the end of normal processing, it precommits and
				 * begins its preparation phase. Precommit simply consists of acquiring the
				 * transaction’s end timestamp and setting the transaction state to Preparing.
				 *
				 */
				return 1;
			}
			else
				return 0;
		} else {
			/*
			 * TE is terminated or not found
			 */
			if(V.isGarbage)
				return 0;
			else {
				cout << "Invalid version!\n";
				throw;
			}
		}
	}
}

/****************************************************************************/
inline bool Transaction::readable(Version* V, bool _not_select){
	int res = checkVisibility(*V);
	while(res == -1){
		res = checkVisibility(*V);
	}
	if(res == 1){
		// adding V to ReadSet if V was called by a select clause
		if(!_not_select)
			ReadSet.push_back(V);
		return 1;
	}
	else
		return 0;
}



Version* Transaction::read(string tableName, Predicate pred, bool _not_select){
	if(tableName == "warehouse"){
		auto pkey = pred.pk_int;
		//register the scan into ScanSet
		ScanSet_Warehouse.push_back(make_pair(&warehouse.pk_index, pkey));
		auto range = warehouse.pk_index.equal_range(pkey);
		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			// V is the scanned version
			Version* V = &(it->second);
			if(readable(V,_not_select))
				return V;
		}
	}
	else if(tableName == "district"){
		auto pkey = pred.pk_2int;
		ScanSet_District.push_back(make_pair(&district.pk_index, pkey));
		auto range = district.pk_index.equal_range(pkey);
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(readable(V,_not_select))
				return V;
		}
	}

	else if(tableName == "customer"){
		auto pkey = pred.pk_3int;
		ScanSet_Customer.push_back(make_pair(&customer.pk_index, pkey));
		auto range = customer.pk_index.equal_range(pkey);
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(readable(V,_not_select))
				return V;
		}
	}
	else if(tableName == "neworder"){
		auto pkey = pred.pk_3int;
		ScanSet_NewOrder.push_back(make_pair(&neworder.pk_index, pkey));
		auto range = neworder.pk_index.equal_range(pkey);
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(readable(V,_not_select))
				return V;
		}
	}
	else if(tableName == "order"){
		auto pkey = pred.pk_3int;
		ScanSet_Order.push_back(make_pair(&order.pk_index, pkey));
		auto range = order.pk_index.equal_range(pkey);
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(readable(V,_not_select))
				return V;
		}
	}

	else if(tableName == "orderline"){
		auto pkey = pred.pk_4int;
		ScanSet_OrderLine.push_back(make_pair(&orderline.pk_index, pkey));
		auto range = orderline.pk_index.equal_range(pkey);
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(readable(V,_not_select))
				return V;
		}
	}

	else if(tableName == "item"){
		auto pkey = pred.pk_int;
		ScanSet_Item.push_back(make_pair(&item.pk_index, pkey));
		auto range = item.pk_index.equal_range(pkey);
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(readable(V,_not_select))
				return V;
		}
	}

	else if(tableName == "stock"){
		auto pkey = pred.pk_2int;
		ScanSet_Stock.push_back(make_pair(&stock.pk_index, pkey));
		auto range = stock.pk_index.equal_range(pkey);
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(readable(V,_not_select))
				return V;
		}
	}
	if(!_not_select)
		cout << "Reading failed from table " << tableName << "!\n";
	return nullptr;	// V not found
}

/****************************************************************************/
Version* Transaction::remove(string tableName, Predicate pred){
	// check if the to be updated version is visible
	Version* V = read(tableName, pred, true);
	if(V){
		auto updatable = checkUpdatibility(*V);

		if(updatable == 0)
			return nullptr;

		// V can be updated
		else if (updatable == 1){
			if( (V->end & 1ull<<63) == 0){ // V's End field is a timestamp
				// set V's End field to Tid
				V->end = Tid;
				// put V into WriteSet
				WriteSet.push_back(make_pair(V,nullptr));
				return V;

			} else{
				// T must abort if there is another Transaction has sneaked into V
				return nullptr;
			}
		}
	}

	return nullptr;
}



/*
 * @param:
 * 		del : delete (true = delete-operation, false = update-operation)
 */
Version* Transaction::update(string tableName, Predicate pred){
	// check if the to be updated version is visible
	Version* V = read(tableName, pred, true);
	if(V){
		int updatable = checkUpdatibility(*V);

		while(updatable == -1)
			updatable = checkUpdatibility(*V); // recheck

		if(updatable == 0)
			return nullptr;

		// V can be updated
		else if (updatable == 1){
			if( (V->end & 1ull<<63) == 0){ // there is no T-id in V's End field
				if(V->begin != Tid) // if V's begin == Tid, then T is updating a new version
					// set V's End field to Tid
					V->end = Tid;
			} else{
				// T must abort if there is another Transaction has sneaked into V
				return nullptr;
			}

			// create a new version
			if(tableName == "warehouse"){
				Warehouse::Tuple* VN = new Warehouse::Tuple();
				*VN = *(dynamic_cast<Warehouse::Tuple*>(V));
				VN->setTime(Tid, INF);
				VN = &(warehouse.pk_index.insert(make_pair(pred.pk_int, *VN))->second);
				WriteSet.push_back(make_pair(V,VN));
				return VN;

			}
			else if (tableName == "district"){
				District::Tuple* VN = new District::Tuple();
				*VN = *(dynamic_cast<District::Tuple*>(V));
				VN->setTime(Tid, INF);
				VN = &(district.pk_index.insert(make_pair(pred.pk_2int, *VN))->second);
				WriteSet.push_back(make_pair(V,VN));
				return VN;
			}
			else if (tableName == "customer"){
				Customer::Tuple* VN = new Customer::Tuple();
				*VN = *(dynamic_cast<Customer::Tuple*>(V));
				VN->setTime(Tid, INF);
				VN = &(customer.pk_index.insert(make_pair(pred.pk_3int, *VN))->second);
				WriteSet.push_back(make_pair(V,VN));
				return VN;
			}
			else if (tableName == "neworder"){
				NewOrder::Tuple* VN = new NewOrder::Tuple();
				*VN = *(dynamic_cast<NewOrder::Tuple*>(V));
				VN->setTime(Tid, INF);
				VN = &(neworder.pk_index.insert(make_pair(pred.pk_3int, *VN))->second);
				WriteSet.push_back(make_pair(V,VN));
				return VN;
			}
			else if (tableName == "order"){
				Order::Tuple* VN = new Order::Tuple();
				*VN = *(dynamic_cast<Order::Tuple*>(V));
				VN->setTime(Tid, INF);
				VN = &(order.pk_index.insert(make_pair(pred.pk_3int, *VN))->second);
				WriteSet.push_back(make_pair(V,VN));
				return VN;
			}
			else if (tableName == "orderline"){
				OrderLine::Tuple* VN = new OrderLine::Tuple();
				*VN = *(dynamic_cast<OrderLine::Tuple*>(V));
				VN->setTime(Tid, INF);
				VN = &(orderline.pk_index.insert(make_pair(pred.pk_4int, *VN))->second);
				WriteSet.push_back(make_pair(V,VN));
				return VN;
			}
			else if (tableName == "item"){
				Item::Tuple* VN = new Item::Tuple();
				*VN = *(dynamic_cast<Item::Tuple*>(V));
				VN->setTime(Tid, INF);
				VN = &(item.pk_index.insert(make_pair(pred.pk_int, *VN))->second);
				WriteSet.push_back(make_pair(V,VN));
				return VN;
			}

			else if (tableName == "stock"){
				Stock::Tuple* VN = new Stock::Tuple();
				*VN = *(dynamic_cast<Stock::Tuple*>(V));
				VN->setTime(Tid, INF);
				VN = &(stock.pk_index.insert(make_pair(pred.pk_2int, *VN))->second);
				WriteSet.push_back(make_pair(V,VN));
				return VN;
			}

		}
	}

	return nullptr;
}



Version* Transaction::insert(string tableName, Predicate pred, Version* VI){
	Version* V = read(tableName, pred, true);
	if(!V){
		if(tableName == "warehouse"){
			VI = &(warehouse.pk_index.insert(make_pair(pred.pk_int, *(dynamic_cast<Warehouse::Tuple*>(VI))))->second);
		}

		else if(tableName == "district"){
			VI = &(district.pk_index.insert(make_pair(pred.pk_2int, *(dynamic_cast<District::Tuple*>(VI))))->second);
		}

		else if(tableName == "customers"){
			VI = &(customer.pk_index.insert(make_pair(pred.pk_3int, *(dynamic_cast<Customer::Tuple*>(VI))))->second);
		}

		else if(tableName == "neworder"){
			VI = &(neworder.pk_index.insert(make_pair(pred.pk_3int, *(dynamic_cast<NewOrder::Tuple*>(VI))))->second);
		}

		else if(tableName == "order"){
			VI = &(order.pk_index.insert(make_pair(pred.pk_3int, *(dynamic_cast<Order::Tuple*>(VI))))->second);
		}

		else if(tableName == "orderline"){
			VI = &(orderline.pk_index.insert(make_pair(pred.pk_4int, *(dynamic_cast<OrderLine::Tuple*>(VI))))->second);
		}

		else if(tableName == "item"){
			VI = &(item.pk_index.insert(make_pair(pred.pk_int, *(dynamic_cast<Item::Tuple*>(VI))))->second);
		}

		else if(tableName == "stock"){
			VI = &(stock.pk_index.insert(make_pair(pred.pk_2int, *(dynamic_cast<Stock::Tuple*>(VI))))->second);
		}

		if(VI->begin == INF && VI->end == INF){
			VI->setTime(Tid, INF);
			WriteSet.push_back(make_pair(nullptr, VI));
			return VI;
		}
		else
			throw;
	}
	else{
//		cout << "Insertion failed from table " << tableName << "!\n";
		return nullptr;

	}

}

bool Transaction::checkPhantom(Version* V){
	if(V->begin != Tid){
		if(V->begin < INF && V->begin > begin
			&& V->end <= INF && V->end > end){
			// phantom detected
			return true;
		}
	}
	return false;
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
		if(v->end < end)
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
	for(auto& p : ScanSet_Warehouse){
		auto range = p.first->equal_range(p.second);
		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(checkPhantom(V))	return -2;
		}
	}

	for(auto& p : ScanSet_District){
		auto range = p.first->equal_range(p.second);
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(checkPhantom(V))	return -2;
		}
	}
	for(auto& p : ScanSet_Customer){
			auto range = p.first->equal_range(p.second);
			//start the scan
			for(auto it = range.first; it != range.second; ++it){
				Version* V = &(it->second);
				//check predicate
				if(checkPhantom(V))	return -2;
			}
		}
	for(auto& p : ScanSet_NewOrder){
		auto range = p.first->equal_range(p.second);
		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			//check predicate
			if(checkPhantom(V))	return -2;
		}
	}
	for(auto& p : ScanSet_Order){
		auto range = p.first->equal_range(p.second);
		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			//check predicate
			if(checkPhantom(V))	return -2;
		}
	}
	for(auto& p : ScanSet_OrderLine){
		auto range = p.first->equal_range(p.second);
		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			//check predicate
			if(checkPhantom(V))	return -2;
		}
	}
	for(auto& p : ScanSet_Item){
		auto range = p.first->equal_range(p.second);
		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			//check predicate
			if(checkPhantom(V))	return -2;
		}
	}
	for(auto& p : ScanSet_Stock){
		auto range = p.first->equal_range(p.second);
		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			//check predicate
			if(checkPhantom(V))	return -2;
		}
	}

	return 1;
}
/****************************************************************************/
void Transaction::abort(int code){
	state = State::Aborted;
	CODE = code;

	/*
	 * -9 : read error
	 * -10 : update error
	 * -11 : delete error
	 * -12 : insert error
	 */
	if(code<=-9 && code >=-12){
		for(auto& pair : WriteSet){

			// reset the end field of the old version
			if(pair.first && pair.first->end == Tid){
				pair.first->end = INF;
			}
			// set the Begin field of the new version to INF to make it invisible
			if(pair.second && pair.second->begin == Tid){
				pair.second->begin = INF;
				pair.second->isGarbage = true;
			}
			/*
			 * another transaction may already have detected the abort,
			 * created another new version and reset the End field of the
			 * old version. If so, TA leaves the End field value unchanged.
			 */
			else {
				continue;
			}
		}

		// all commit dependent Transactions also have to abort
		for(auto& t : CommitDepSet){
			auto itr = TransactionManager.find(t);
			if( itr != TransactionManager.end()){
				// TD: Dependent Transaction
				auto TD = itr->second;
				// -19: cascaded abort code
				TD->AbortNow = true;
			} else {
				throw;
			}
		}
//		cout << "Transaction " << (Tid - (1ull<<63)) << ", Code = " << (CODE) << ", begin = " << begin << ", end = " << end << "\n";

		GarbageTransactions.push_back(this);
		TransactionManager.erase(Tid);
	}


}

void Transaction::commit(){
	/*
	 * write log records here
	 */
	if(state == State::Preparing)
		state = State::Committed;
}


void Transaction::precommit(){
	if(state == State::Active){
		state = State::Preparing;
		end = getTimestamp();
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



/*****************************************************************************/

void Transaction::execute(int z){
	/*-----------------------------------------Randomize the inputs-------------------------------------------------*/

	/*------------------------------------------Execute the transaction newOrder-------------------------------------*/

	/*
	 * Begin the NORMAL PROCESSING PHASE
	 */
	/*
	 * Use while(true) and breaks to stop the Transaction to run further because the statement cannot be
	 * executed (e.g. try to read a non-existing/invisible version, insert with the duplicate primary key, ...).
	 *
	 * In this case of the TPPC-neworder-transaction, using while(true) and break is still OK, but for more
	 * general cases with transactions having nested loops in it, we should use GOTO and LABEL instead.
	 */

	for(auto& ol : orderline.pk_index){
	}
	/*------------------------------------------Begin the MVCC mechanism-----------------------------------------------*/

	/*
	 * End of NORMAL PROCESSING PHASE
	 */
	if(state==State::Active){
		if(!AbortNow)
			precommit();
		else
			abort(-19);
	}


	/*
	 * Begin of PREPARATION PHASE
	 */

	//Testing cascaded abort and commit dependencies
//	if((Tid - (1ull<<63)) == 1){
//		cout << "Transaction " << (Tid - (1ull<<63))  << " sleeps for 300s... \n";
//		std::this_thread::sleep_for(std::chrono::seconds(300));
////		abort(-100);
//	}

	/*
	 * VALIDATION
	 */
	if(state==State::Preparing){
		int valid = 0;
		if(!AbortNow){
			valid = validate();
			if( valid > 0 && !AbortNow){
				// the Transaction must wait here for commit dependencies if there are any
				while(CommitDepCounter!=0 && state!=State::Aborted){
					cout << "Transaction " << (Tid - (1ull<<63))  << " is waiting for CommitDep... \n";
					std::this_thread::sleep_for(std::chrono::milliseconds(500));
				}
				//only allowed to commit if AbortNow not set and there is no CommitDep left
				if(!AbortNow && CommitDepCounter == 0){
					commit();
				}
			}
			else
				abort(valid);
		}
		else {
			abort(-19);
		}
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
	if(state == State::Committed){
		for(auto& pair : WriteSet){
			// for the old version
			if(pair.first){
				pair.first->end = end;
				pair.first->isGarbage = true;
			}
			// for the new version
			if(pair.second){
				pair.second->begin = getTimestamp();
			}
		}
		// handle commit dependencies
		for(auto& t : CommitDepSet){
			try{
				auto TD = TransactionManager.at(t);
				--(TD->CommitDepCounter);
				if(TD->CommitDepCounter == 0){
					//wake TD up
				}

			} catch(out_of_range& oor){
				// the dependent Transaction does not exist or is terminated
				continue;
			}
		}

		CODE = 1;
	}

	else if(state == State::Aborted){
		for(auto& pair : WriteSet){
			// for the old version
			/*
			 * another transaction may already have detected the abort,
			 * created another new version and reset the End field of the
			 * old version. If so, TA leaves the End field value unchanged.
			 */
			if(pair.first && pair.first->end == Tid){
				pair.first->end = INF;
			}
			// for the new version
			if(pair.second){
				pair.second->begin = INF;
				pair.second->isGarbage = true;
			}
		}

		// all commit dependent Transactions also have to abort
		for(auto& t : CommitDepSet){
			auto itr = TransactionManager.find(t);
			if( itr != TransactionManager.end()){
				auto TD = itr->second;
				// -19: cascaded abort code
				TD->abort(-19);
			} else throw;
		}
	}
	else
		throw;

//	if(CODE > -9 || CODE <-12)
//		cout << "Transaction " << (Tid - (1ull<<63)) << ", Code = " << (CODE) << ", begin = " << begin << ", end = " << end << "\n";

	GarbageTransactions.push_back(this);
	TransactionManager.erase(Tid);
}
