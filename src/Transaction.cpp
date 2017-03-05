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

// counter to generate the Transactions ID
atomic<uint64_t> GMI_tid{1ull<<63};

uint64_t getTid(){
	return ++GMI_tid;
}

unordered_map<uint64_t,Transaction*> TransactionManager;

vector<Transaction*> GarbageTransactions;






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
		try{
			if(V.begin == T->Tid && V.end == INF){
				// for updating a version several times by a same transaction OR
				// for visibility validating a version that first was read (in ReadSet) and
				// then was updated by the same transaction.
				// Because the updating set V's Begin field to the Tid
				// just read V
				return 1;
			}
			auto TB = TransactionManager.at(V.begin);
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
		} catch (out_of_range& oor){
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
		try{
//			if(V.begin < RT && V.end == T->Tid){
//				// for updating a version several times by a same transaction OR
//				// for visibility validating a version that first was read (in ReadSet) and
//				// then was updated by the same transaction.
//				// Because the updating set V's Begin field to the Tid
//				// just read V
//				return 1;
//			}
			auto TE = TransactionManager.at(V.end);
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

		} catch (out_of_range& oor){
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
int Transaction::checkUpdatibility(Version& V){
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
// WRITE TEMPLATE FOR RANGE HERE

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
		auto range = warehouse.pk_index.equal_range(pred.pk_int);
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
		auto range = orderline.pk_index.equal_range(pred.pk_4int);
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(readable(V,_not_select))
				return V;
		}
	}

	else if(tableName == "item"){
		auto pkey = pred.pk_int;
		ScanSet_Item.push_back(make_pair(&item.pk_index, pkey));
		auto range = item.pk_index.equal_range(pred.pk_int);
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(readable(V,_not_select))
				return V;
		}
	}

	else if(tableName == "stock"){
		auto pkey = pred.pk_2int;
		ScanSet_Stock.push_back(make_pair(&stock.pk_index, pkey));
		auto range = stock.pk_index.equal_range(pred.pk_2int);
		for(auto it = range.first; it != range.second; ++it){
			Version* V = &(it->second);
			if(readable(V,_not_select))
				return V;
		}
	}
	if(!_not_select)
		cout << "A version from table " << tableName << " is not readable!\n";
	else
		cout << "A version from table " << tableName << " is not readable for updating!\n";
	return nullptr;	// V not found
}

/****************************************************************************/
/*
 * @param:
 * 		del : delete (true = delete-operation, false = update-operation)
 */
Version* Transaction::update(string tableName, Predicate pred, bool del){
	// check if the to be updated version is visible
	Version* V = read(tableName, pred, true);
	if(V){
		auto updatable = checkUpdatibility(*V);

		while(updatable == -1)
			updatable = checkUpdatibility(*V); // recheck

		if(updatable == 0)
			return nullptr;

		// V can be updated
		else if (updatable == 1){
			if( (V->end & 1ull<<63) == 0){ // there is no T-id in V's End field
				// set V's End field to Tid
				V->end = Tid;
				// for deleting
				if(del){
					WriteSet.push_back(make_pair(V,nullptr));
					return V;
				}
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

			else if (tableName == "stock"){
				Stock::Tuple* VN = new Stock::Tuple();
				*VN = *(dynamic_cast<Stock::Tuple*>(V));
				VN->setTime(Tid, INF);
				VN = &(stock.pk_index.insert(make_pair(pred.pk_2int, *VN))->second);
				WriteSet.push_back(make_pair(V,VN));
				return VN;
			}

			else if (tableName == "order"){

			}

		}
	}
	// V is nullptr here
	return V;

}



Version* Transaction::insert(string tableName, Version* VI, Predicate pred){
	//	Version* VN = read(tableName, pred, true);
	if(tableName == "warehouse"){
		auto pkey = pred.pk_int;
		ScanSet_Warehouse.push_back(make_pair(&warehouse.pk_index, pkey));
		auto range = warehouse.pk_index.equal_range(pkey);

		for(auto it = range.first; it != range.second; ++it){
			// the scanned version
			Warehouse::Tuple* V = &(it->second);
			// check predicate and check Visibility of the Version that has the same predicate
			if(V->w_id == pkey && checkVisibility(*V) == 1){
					return nullptr; // not insertable
			}
		}
		VI = &(warehouse.pk_index.insert(make_pair(pkey, *(dynamic_cast<Warehouse::Tuple*>(VI))))->second);
	}

	else if(tableName == "neworder"){
		auto pkey = pred.pk_3int;
		ScanSet_NewOrder.push_back(make_pair(&neworder.pk_index, pkey));
		auto range = neworder.pk_index.equal_range(pkey);

		for(auto it = range.first; it != range.second; ++it){
			// the scanned existing version
			NewOrder::Tuple* V = &(it->second);
			// check predicate and check Visibility of the Version that has the same predicate
			auto tup = make_tuple(V->no_o_id, V->no_d_id, V->no_w_id);
			if(tup == pkey && checkVisibility(*V) == 1){
				return nullptr;	// not insertable
			}
		}
		VI = &(neworder.pk_index.insert(make_pair(pkey, *(dynamic_cast<NewOrder::Tuple*>(VI))))->second);
	}

	else if(tableName == "order"){
		auto pkey = pred.pk_3int;
		ScanSet_Order.push_back(make_pair(&order.pk_index, pkey));
		auto range = order.pk_index.equal_range(pkey);

		for(auto it = range.first; it != range.second; ++it){
			// the scanned version
			Order::Tuple* V = &(it->second);
			// check predicate and check Visibility of the Version that has the same predicate
			auto tup = make_tuple(V->o_id, V->o_d_id, V->o_w_id);
			if(tup == pkey && checkVisibility(*V) == 1){
				return nullptr;	// not insertable
			}
		}
		VI = &(order.pk_index.insert(make_pair(pkey, *(dynamic_cast<Order::Tuple*>(VI))))->second);
	}

	else if(tableName == "orderline"){
		auto pkey = pred.pk_4int;
		ScanSet_OrderLine.push_back(make_pair(&orderline.pk_index, pkey));
		auto range = orderline.pk_index.equal_range(pkey);

		for(auto it = range.first; it != range.second; ++it){
			// the scanned version
			OrderLine::Tuple* V = &(it->second);
			// check predicate and check Visibility of the Version that has the same predicate
			auto tup = make_tuple(V->ol_o_id, V->ol_d_id, V->ol_w_id, V->ol_number);
			if(tup == pkey && checkVisibility(*V) == 1){
				return nullptr;	// not insertable
			}
		}
		VI = &(orderline.pk_index.insert(make_pair(pkey, *(dynamic_cast<OrderLine::Tuple*>(VI))))->second);
	}

	if(VI->begin == INF && VI->end == INF){
		VI->setTime(Tid, INF);
		WriteSet.push_back(make_pair(nullptr, VI));
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
		//if(v->end < INF  && v->end < end)
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
			if((it->second).begin != Tid){
				if((it->second).w_id == v.second
						&& (it->second).begin < INF
						&& (it->second).begin > begin
						&& (it->second).end <= INF
						&& (it->second).end > end){
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
			try{
				// TD: Dependent Transaction
				auto TD = TransactionManager.at(t);
				// -19: cascaded abort code
				TD->AbortNow = true;
			} catch(out_of_range& oor){
				// the dependent Transaction does not exist or is terminated
				continue;
			}
		}
//		cout << "Transaction " << (Tid - (1ull<<63)) << ", Code = " << (CODE) << ", begin = " << begin << ", end = " << end << "\n";

		GarbageTransactions.push_back(this);
		TransactionManager.erase(Tid);

//		std::terminate();
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

void Transaction::execute(int i){
	/***************************************************************************************************/
	/*
	 * Randomize the input
	 */

	int32_t w_id=urand(1,warehouses);
	int32_t d_id=urand(1,10);
	int32_t c_id=nurand(1023,1,3000);
	int32_t ol_cnt=urand(5,15);

	int32_t items = ol_cnt;

	int32_t supware[15];
	int32_t itemid[15];
	int32_t qty[15];
	for (int32_t i=0; i<ol_cnt; i++) {
		if (urand(1,100)>1)
			supware[i]=w_id;
		else
			supware[i]=urandexcept(1,warehouses,w_id);
		itemid[i]=nurand(8191,1,100000);
		qty[i]=urand(1,10);
	}
	Timestamp datetime = 0;

	/*
	 * Begin the NORMAL PROCESSING PHASE
	 */
	Warehouse::Tuple* _warehouse_version;
	District::Tuple* _district_version;
	Customer::Tuple* _customer_version;
	NewOrder::Tuple* _neworder_version;
	Order::Tuple* _order_version;
	OrderLine::Tuple* _orderline_version;
	Item::Tuple* _item_version;
	Stock::Tuple* _stock_version;

	if(i==-1){


		/*
		 * Use while(true) and breaks to stop the Transaction to run further because the statement cannot be
		 * executed (e.g. try to read a non-existing/invisible version, insert with the duplicate primary key, ...).
		 *
		 * In this case of the TPPC-neworder-transaction, using while(true) and break is still OK, but for more
		 * general cases with transactions having nested loops in it, we should use GOTO and LABEL instead.
		 */
		while(true){
			_warehouse_version = dynamic_cast<Warehouse::Tuple*>(read("warehouse", Predicate(w_id), false));
			if(!_warehouse_version) {abort(-9); break;}	// version not found
			auto w_tax = _warehouse_version->w_tax;

			_customer_version = dynamic_cast<Customer::Tuple*>(read("customer", Predicate(c_id, d_id, w_id), false));
			if(!_customer_version) {abort(-9); break;}	// version not found
			auto c_discount = _customer_version->c_discount;

			_district_version = dynamic_cast<District::Tuple*>(read("district", Predicate(d_id, w_id), false));
			if(!_district_version) {abort(-9); break;} // version not found
			auto o_id = _district_version->d_next_o_id;
			auto d_tax = _district_version->d_tax;

			int32_t all_local = 1;
			for(int index = 0; index < items; ++index){
				if(w_id != supware[index])
					all_local = 0;
			}


			for(int index = 0; index < items; ++index){

				_item_version = dynamic_cast<Item::Tuple*>(read("item", Predicate(itemid[index]), false));
				if(!_item_version) {abort(-9); break;}	// version not found
				auto i_price = _item_version->i_price;

				auto s_i_id = itemid[index];
				auto s_w_id = supware[index];
				_stock_version = dynamic_cast<Stock::Tuple*>(read("stock", Predicate(s_i_id, s_w_id), false));
				if(!_stock_version) {abort(-9); break;}	// version cannot be updated
				auto s_quantity = _stock_version->s_quantity;
				auto s_remote_cnt = _stock_version->s_remote_cnt;
				auto s_order_cnt = _stock_version->s_order_cnt;
				Char<24> s_dist;
				switch(d_id){
					case 1 : s_dist = _stock_version->s_dist_01; break;
					case 2 : s_dist = _stock_version->s_dist_02; break;
					case 3 : s_dist = _stock_version->s_dist_03; break;
					case 4 : s_dist = _stock_version->s_dist_04; break;
					case 5 : s_dist = _stock_version->s_dist_05; break;
					case 6 : s_dist = _stock_version->s_dist_06; break;
					case 7 : s_dist = _stock_version->s_dist_07; break;
					case 8 : s_dist = _stock_version->s_dist_08; break;
					case 9 : s_dist = _stock_version->s_dist_09; break;
					case 10 : s_dist = _stock_version->s_dist_10; break;
				}

				if(s_quantity > qty[index]){
					_stock_version = dynamic_cast<Stock::Tuple*>(update("stock",Predicate(s_i_id, s_w_id), false));
					if(!_stock_version) {abort(-10); break;}	// version cannot be updated
					_stock_version->s_quantity = s_quantity - qty[index];
				}
				else {
					_stock_version = dynamic_cast<Stock::Tuple*>(update("stock",Predicate(s_i_id, s_w_id), false));
					if(!_stock_version) {abort(-10); break;}	// version cannot be updated
					_stock_version->s_quantity = s_quantity + 91 - qty[index];
				}

				if(supware[index] != w_id){
					_stock_version = dynamic_cast<Stock::Tuple*>(update("stock",Predicate(s_i_id, w_id), false));
					if(!_stock_version) {abort(-10); break;}	// version cannot be updated
					_stock_version->s_remote_cnt = s_remote_cnt + 1;
				}
				else{
					_stock_version = dynamic_cast<Stock::Tuple*>(update("stock",Predicate(s_i_id, w_id), false));
					if(!_stock_version) {abort(-10); break;}	// version cannot be updated
					_stock_version->s_order_cnt = s_order_cnt + 1;
				}


				Numeric<6,2> ol_amount = qty[index] * i_price.getDouble() * (1 + w_tax.getDouble() + d_tax.getDouble()) * (1 - c_discount.getDouble());

				_orderline_version = dynamic_cast<OrderLine::Tuple*>(insert("orderline",
						new OrderLine::Tuple(o_id, d_id, w_id, index+1, itemid[index], supware[index], 0, qty[index], ol_amount, s_dist),
						Predicate(o_id, d_id, w_id)));

				if(!_orderline_version) {abort(-12); break;} // version not insertable

			}

			_order_version = dynamic_cast<Order::Tuple*>(insert("order", new Order::Tuple(o_id, d_id, w_id, c_id, datetime, 0, items, all_local), Predicate(o_id, d_id, w_id)));
			if(!_order_version) {abort(-12); break;} // version not insertable
			_neworder_version = dynamic_cast<NewOrder::Tuple*>(insert("neworder", new NewOrder::Tuple(o_id, d_id, w_id), Predicate(o_id, d_id, w_id)));
			if(!_neworder_version) {abort(-12); break;} // version not insertable

			break;
		}
	}


	else if(i==1){
		Warehouse::Tuple* V = dynamic_cast<Warehouse::Tuple*>(read("warehouse", Predicate(1), false));
		begin;
		if(!V){
			if(CommitDepCounter == 0)
				abort(-9);
			else
				abort(-8);
		}
		else{
			cout << "V is read! ";

		}
	}

	else if(i==2){
		Warehouse::Tuple* V = dynamic_cast<Warehouse::Tuple*>(update("warehouse", Predicate(1), true));
		if(!V){
			abort(-10);
		}
		else
			cout << "Transaction " << (Tid - (1ull<<63))  << " delete-operation done.\n";
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


		Warehouse::Tuple* row;
		row->w_id = 6;
		row->w_name = row->w_name.castString(string("anhmt").c_str(), string("anhmt").length());
		row->w_street_1 = row->w_street_1.castString(string("a").c_str(), string("a").length());
		row->w_street_2 = row->w_street_2.castString(string("b").c_str(), string("b").length());
		row->w_city = row->w_city.castString(string("c").c_str(), string("c").length());
		row->w_state = row->w_state.castString(string("d").c_str(), string("d").length());
		row->w_zip = row->w_zip.castString(string("e").c_str(), string("e").length());
		row->w_tax = Numeric<4, 4>(1);
		row->w_ytd = Numeric<12, 2>(1);
		row->setTime(Tid, INF);
		auto V = dynamic_cast<Warehouse::Tuple*>(insert("warehouse", row,  Predicate(row->w_id)));
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
		Warehouse::Tuple* row;
		row->w_id = 7;
		row->w_name = row->w_name.castString(string("anhmt").c_str(), string("anhmt").length());
		row->w_street_1 = row->w_street_1.castString(string("a").c_str(), string("a").length());
		row->w_street_2 = row->w_street_2.castString(string("b").c_str(), string("b").length());
		row->w_city = row->w_city.castString(string("c").c_str(), string("c").length());
		row->w_state = row->w_state.castString(string("d").c_str(), string("d").length());
		row->w_zip = row->w_zip.castString(string("e").c_str(), string("e").length());
		row->w_tax = Numeric<4, 4>(1);
		row->w_ytd = Numeric<12, 2>(1);
		row->setTime(Tid, INF);
		auto V = dynamic_cast<Warehouse::Tuple*>(insert("warehouse", row,  Predicate(row->w_id)));
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
				while(CommitDepCounter!=0 && !(AbortNow)){
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
				TD->decreaseCommitDepCounter();
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
			//for deleting
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

	else
		throw;

//	if(CODE > -9 || CODE<-12)
//		cout << "Transaction " << (Tid - (1ull<<63)) << ", Code = " << (CODE) << ", begin = " << begin << ", end = " << end << "\n";

	GarbageTransactions.push_back(this);
	TransactionManager.erase(Tid);
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
