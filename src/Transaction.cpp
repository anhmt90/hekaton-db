/*
 * Transaction.cpp
 *
 *  Created on: Jan 24, 2017
 *      Author: anhmt90
 */

#include "Transaction.hpp"
#include <unistd.h>

using namespace std;

void newOrderRandom(Timestamp);

/****************************************************************************/
uint64_t GMI_tid = (1ull<<63) + 1;

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
int checkVisibility(Version& V, Transaction* T){
	// logical read time
	auto RT = T->begin;
	// CHECK VISIBILITY OF VERSIONS READ
	/*
	 * 1st case: Begin and End fields contain timestamps
	 * only versions with End field = INF are visible
	 */

	if((V.begin & 1ull<<63) == 0 && (V.end & 1ull<<63) == 0){
		if(V.begin < RT && RT < V.end){
			// V is visible to this transaction
			// just read the version
			return 1;
		}
		else
			return 0;
	}


	/*
	 * 2nd case: V's Begin field contains a transaction ID (of TB)
	 */
	else if ((V.begin & 1ull<<63) != 0 && (V.end & 1ull<<63) == 0){
		try{
			auto TB = TransactionManager.at(V.begin);
			auto TS = TB->end;

			if(TB->state == Transaction::State::Active && TS == NOT_SET){
				if(TB->Tid == T->Tid && V.end == INF)
				// for updating a version several time within a transaction
				// just read V
					return 1;
				else
					return 0;
			}

			else if(TB->state == Transaction::State::Preparing && TS != NOT_SET){
				if(V.begin < TS && V.end == INF){
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
	else if (V.begin < RT && (V.end & 1ull<<63) != 0){
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
int checkUpdatibility(Version& V, Transaction* T){
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
Version* Transaction::read(string tableName, Predicate pred){
	if(tableName == "warehouse"){
		auto pkey = pred.pk_int;
		//register the scan into ScanSet
		ScanSet_Warehouse.push_back(make_pair(&warehouse.pk_index, pkey));
		auto range = warehouse.pk_index.equal_range(pred.pk_int);

		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			// the scanned version
//			Warehouse_Tuple V = new Warehouse_Tuple(this->Tid);
			Warehouse_Tuple* V = &(it->second);
			//check predicate
			if(V->w_id != pkey)
				continue;
			else {
				int res = checkVisibility(*V, this);
				while(res == -1){
					res = checkVisibility(*V, this);
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
	else if(tableName == "orderline"){
		auto pkey = pred.pk_4int;
		ScanSet_OrderLine.push_back(make_pair(&orderline.pk_index, pkey));
		auto range = orderline.pk_index.equal_range(pred.pk_4int);

		for(auto it = range.first; it != range.second; ++it){
			// the scanned version
			OrderLine_Tuple* V = &(it->second);
			//check predicate
			if(make_tuple(V->ol_o_id, V->ol_d_id, V->ol_w_id, V->ol_number) != pkey)
				continue;
			else {
				int res = checkVisibility(*V, this);
				while(res == -1){
					res = checkVisibility(*V, this);
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
Version* Transaction::update(string tableName, Predicate pred){
	Version* V = read(tableName, pred);
	if(V){
		auto updatable = checkUpdatibility(*V, this);

		while(updatable == -1)
			updatable = checkUpdatibility(*V, this); // recheck

		if(updatable == 0)
			return nullptr;

		// V can be updated
		else if (updatable == 1){
			if( (V->end & 1ull<<63) == 0){
				V->end = this->Tid;		// set V's End field to Tid
			}
			else
				// T must abort if there is another Transaction has sneaked into V
				return nullptr;

			// create a new version
			if(tableName == "warehouse"){
				Warehouse_Tuple* VN = new Warehouse_Tuple(NOT_SET);
				*VN = *(dynamic_cast<Warehouse_Tuple*>(V));

//				*VN = *V;
				VN->begin = this->Tid;
				VN->end = INF;

//				Warehouse_Tuple* VN = dynamic_cast<Warehouse_Tuple*>(V);

				this->WriteSet.push_back(make_pair(V,VN));
				return VN;
//				if(VN){
//					vn->
//					Warehouse_Tuple* i = Warehouse_Table::insert(*vn);
//					return VN;
//				}

			} else if (tableName == "orderline"){

			}

		}
	}
	return V;

}

/****************************************************************************/
bool Transaction::validate(){
	/*
	 * Validation consists of two steps: checking visibility of the
	 * versions read and checking for phantoms.
	 */

	/*
	 * To check visibility transaction T scans its ReadSet and for
	 * each version read, checks whether the version is still visible
	 * as of the end of the transaction.
	 */

	for(auto v : ReadSet){
		if(v->end < this->end){
			return false;
		}
	}

	/*
	 * To check for phantoms, T walks its ScanSet and repeats each scan
	 * looking for versions that came into existence during T’s lifetime
	 * and are visible as of the end of the transaction.
	 */
	for(auto& v : ScanSet_Warehouse){
		auto range = v.first->equal_range(v.second);

		//start the scan
		for(auto it = range.first; it != range.second; ++it){
			//check predicate
			if((it->second).w_id == v.second && (it->second).begin > this->begin && (it->second).end > this->end){
				// phantom detected
				return false;
			}
		}
	}

	return true;
}
/****************************************************************************/
void Transaction::abort(){
	this->state = State::Aborted;
	this->AbortNow = true;
}

void Transaction::commit(){
	/*
	 * write log records here
	 */

	this->state = State::Committed;
}


void Transaction::precommit(){
	this->state = State::Preparing;
	this->end = getTimestamp();
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



void Transaction::execute(){
	newOrderRandom(0);

	/*
	 * Randomize the input
	 */

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
	Timestamp datetime = 0;
	/***************************************************************************************************/
	/*
	 * update district
	 * set d_next_o_id=o_id+1
	 * where d_w_id=w_id and district.d_id=d_id;
	 */

	/*
	 * Begin the NORMAL PROCESSING PHASE
	 */
	Warehouse_Tuple* VN = dynamic_cast<Warehouse_Tuple*>(update("warehouse", Predicate((Integer) 4)));
	if(!VN)
		cout << "VN is null" << "\n";
	else{
		VN->w_street_1 = Varchar<20>::castString(string("changed2something").c_str(), string("changed2something").length());
	}
//		cout << "\n" << VN->begin << " " << VN->end << " | " << VN->w_id <<"\t" << VN->w_street_1 << "\n";

	/***************************************************************************************************/
	/*
	 * End of NORMAL PROCESSING PHASE
	 */
	if(this->state != State::Aborted)
		precommit();

	/*
	 * Begin of PREPARATION
	 */

	/*
	 * VALIDATION
	 */
	if(validate()){
		while(!this->AbortNow && CommitDepCounter!=0){
			//usleep(200000);
		}
		if(!this->AbortNow && CommitDepCounter == 0){
			commit();
		}
	}
	else
		abort();

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
			pair.first->end = this->end;
			// for the new version
			pair.second->begin = this->end;
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
	}


	else if(this->state == State::Aborted){
		for(auto& pair : this->WriteSet){
			// for the old version
			/*
			 * another transaction may already have detected the abort,
			 * created another new version and reset the End field of the
			 * old version. If so, TA leaves the End field value unchanged.
			 */
			if(pair.first->end == this->Tid){
				pair.first->end = INF;
			}
			// for the new version
			pair.second->begin = INF;
			pair.second->isGarbage = true;
		}

		// all commit dependent Transactions also have to abort
		for(auto& t : CommitDepSet){
			try{
				auto TD = TransactionManager.at(t);
				TD->abort();
			} catch(out_of_range& oor){
				// the dependent Transaction does not exist or is terminated
				continue;
			}
		}
	}

	else
		throw;

	GarbageTransactions.push_back(this);
	TransactionManager.erase(this->Tid);
}










/*****************************************************************************************************************/



void newOrder(int32_t w_id, int32_t d_id, int32_t c_id, int32_t items, int32_t supware[15],
		int32_t itemid[15], int32_t qty[15], Timestamp datetime){
	try{

		//		//Select
		//		auto i_w = (tpcc->warehouse).at(w_id);
		//		auto w_tax = (tpcc->warehouse)[i_w].w_tax.value;
		//
		//		auto i_c = (tpcc->customer_ix).at(make_tuple((Integer)c_id, (Integer) d_id, (Integer) w_id));
		//		auto c_discount = (tpcc->customer)[i_c].c_discount.value;
		//
		//		auto i_d = (tpcc->district_ix).at(make_tuple((Integer)d_id, (Integer)w_id));
		//		auto o_id = (tpcc->district)[i_d].d_next_o_id.value;
		//		auto d_tax = (tpcc->district)[i_d].d_tax.value;
		//
		//		//Update
		//		(tpcc->district)[i_d].d_next_o_id = o_id+1;
		//
		//		int64_t all_local = 1;
		//
		//		for(int32_t index = 0; index < items; index++){
		//			if(w_id != supware[index])
		//				all_local = 0;
		//		}
		//
		//		//Insert
		//		tpcc->TPCC::o_Insert(make_tuple((Integer)o_id, (Integer)d_id, (Integer)w_id), *(new o_Row(o_id, d_id, w_id,c_id,
		//				datetime,0,items,all_local)));
		//		tpcc->no_Insert(make_tuple((Integer)o_id, (Integer)d_id, (Integer)w_id), *(new no_Row((Integer)o_id, (Integer)d_id, (Integer)w_id)));
		//
		//		for(int32_t index = 0; index < items; index++){
		//			auto i_i = (tpcc->item_ix).at(itemid[index]);
		//			auto i_price = (tpcc->item)[i_i].i_price.value;
		//
		//			auto i_s = (tpcc->stock_ix).at(make_tuple(itemid[index],supware[index]));
		//			auto s_dist = (tpcc->stock)[i_s].s_dist_01;
		//			switch(d_id){
		//			case 2: s_dist = (tpcc->stock)[i_s].s_dist_02; break;
		//			case 3: s_dist = (tpcc->stock)[i_s].s_dist_03; break;
		//			case 4: s_dist = (tpcc->stock)[i_s].s_dist_04; break;
		//			case 5: s_dist = (tpcc->stock)[i_s].s_dist_05; break;
		//			case 6: s_dist = (tpcc->stock)[i_s].s_dist_06; break;
		//			case 7: s_dist = (tpcc->stock)[i_s].s_dist_07; break;
		//			case 8: s_dist = (tpcc->stock)[i_s].s_dist_08; break;
		//			case 9: s_dist = (tpcc->stock)[i_s].s_dist_09; break;
		//			case 10: s_dist = (tpcc->stock)[i_s].s_dist_10; break;
		//			default: s_dist = (tpcc->stock)[i_s].s_dist_01; break;
		//			}
		//
		//			//cout << selection.s_quantity <<"\t"<< selection.s_remote_cnt <<"\t"<< selection.s_order_cnt <<"\t"<< s_dist << "\n";
		//			if((tpcc->stock)[i_s].s_quantity > qty[index]){
		//				(tpcc->stock)[i_s].s_quantity = (tpcc->stock)[i_s].s_quantity - qty[index];
		//			} else {
		//				(tpcc->stock)[i_s].s_quantity = (tpcc->stock)[i_s].s_quantity + 91 - qty[index];
		//			}
		//
		//			i_s = (tpcc->stock_ix).at(make_tuple(itemid[index],w_id));
		//			if(supware[index] != w_id){
		//				(tpcc->stock)[i_s].s_remote_cnt+=1;
		//			} else {
		//				(tpcc->stock)[i_s].s_order_cnt+=1;
		//			}
		//
		//			auto ol_amount = (qty[index] * (i_price) * (1.0 + w_tax + d_tax) * (1.0 - c_discount));
		//
		//			tpcc->ol_Insert(make_tuple((Integer)o_id, (Integer)d_id, (Integer)w_id, (Integer)index+1),
		//					*(new ol_Row((Integer)o_id, (Integer)d_id, (Integer)w_id, (Integer)index+1, itemid[index], supware[index],0, qty[index], ol_amount, s_dist)));


		//	}
	}catch(out_of_range &oor){
		//c++;
	}

}

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

	newOrder(w_id,d_id,c_id,ol_cnt,supware,itemid,qty,now);
}
