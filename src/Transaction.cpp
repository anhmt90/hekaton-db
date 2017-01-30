/*
 * Transaction.cpp
 *
 *  Created on: Jan 24, 2017
 *      Author: anhmt90
 */

#include "Transaction.hpp"

using namespace std;

void newOrderRandom(Timestamp);

uint64_t GMI_tid = (1ull<<63) + 1;

unordered_map<uint64_t,Transaction*> TransactionManager;

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
				// just read V
					return 1;
				else
					return 0;
			}

			else if(TB->state == Transaction::State::Preparing && TS != NOT_SET){
				if(V.begin < TS && V.end == INF){
					//SPECULATIVELY read V
					++(T->CommitDepCounter);
					TB->CommitDepSet.insert(make_pair(T->Tid, T));
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

			else if(TB->state == Transaction::State::Arborted){
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
					TE->CommitDepSet.insert(make_pair(T->Tid, T));
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
			else if(TE->state == Transaction::State::Arborted){
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

bool checkUpdatibility(Version& V, Transaction* T){
	// V.end is a timestamp
	if((V.end & 1ull<<63) == 0) {
		if(V.end == INF)
			return true;
		else
			return false;
	}
	// V.end contains a Tid
	else {
		try{
			auto TE = TransactionManager.at(V.end);
			if(TE->state == Transaction::State::Arborted)
				return true;
			else
				return false;
		} catch (out_of_range& oor){
			/*
			 * TE is terminated or not found
			 */
			return true;
		}
	}
}

void Transaction::lookup(string tableName, Predicate pred){
//	pair<Warehouse::iterator, Warehouse::iterator> range;
	if(tableName == "warehouse"){
		auto pkey = pred.pk_int;
		//register the scan into ScanSet
		ScanSet_Warehouse.push_back(make_pair(&warehouse, pkey));
		auto range = warehouse.equal_range(pred.pk_int);

		//start the scan
		for(auto it = range.first; it != range.second; ++it){
				// the scanned version
				auto V = it->second;
				//check predicate
				if(V.w_id != pkey)
					continue;
				else {
					int res = checkVisibility(V, this);
					while(res == -1){
						res = checkVisibility(V, this);
					}
					if(res == 1)
						this->ReadSet.push_back(&V);
					else
						continue;
				}


		}
	}
	else if(tableName == "orderline"){
		auto pkey = pred.pk_4int;
		ScanSet_OrderLine.push_back(make_pair(&orderline, pkey));
		auto range = orderline.equal_range(pred.pk_4int);
		for(auto it = range.first; it != range.second; ++it){
			// the scanned version
			auto V = it->second;
			//check predicate
			if(make_tuple(V.ol_o_id, V.ol_d_id, V.ol_w_id, V.ol_number) != pkey)
				continue;
			else {
				int res = checkVisibility(V, this);
				while(res == -1){
					res = checkVisibility(V, this);
				}
				if(res == 1)
					this->ReadSet.push_back(&V);
				else
					continue;
			}


		}
	}
}

//void Transaction::insert(string tableName, Predicate pred){
//
//}


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
	/**********************************************************************/



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
