/*
 * Transaction.hpp
 *
 *  Created on: Jan 23, 2017
 *      Author: anhmt90
 */

#ifndef SRC_TRANSACTION_HPP_
#define SRC_TRANSACTION_HPP_

#include <climits>	//for CHAR_BIT
#include <set>
#include <cstdint>

#include "Schema.hpp"

using namespace std;

struct Transaction;

const uint64_t NOT_SET = 1ull<<63;

extern uint64_t GMI_tid;
extern unordered_map<uint64_t,Transaction*> TransactionManager;



class TransactionNotFoundException : public exception {
public:
	TransactionNotFoundException(){}
};

struct Predicate {
	Integer pk_int;
	tup_2Int pk_2int;
	tup_3Int pk_3int;
	tup_4Int pk_4int;
};

struct Transaction{
	// Transaction ID
	const uint64_t Tid;
	// begin time of the transaction, also used as logical read time
	uint64_t begin;
	/*
	 *  end time of the transaction, will be set as INF in the constructor,
	 *  will be set by getTimestamp() again when the transaction precommits.
	 */
	uint64_t end;

	/*
	 * counts how many unresolved commit deps this transaction still has.
	 * A transaction cannot commit until this counter = 0
	 */
	int CommitDepCounter = 0;

	/*
	 * other transactions can set this variable to tell this transaction
	 * to abort.
	 */
	bool AbortNow = false;

	/*
	 * stores Tid of the transactions that depend on this transaction
	 */
	unordered_map<uint64_t, Transaction*> CommitDepSet;

	/*
	 *
	 */
	vector<Version*> ReadSet;
	vector<pair<Warehouse*,Integer>> ScanSet_Warehouse;
	vector<pair<OrderLine*,tup_4Int>> ScanSet_OrderLine;
	vector<Version*> WriteSet;

	/* each transaction can be in 1 of 4 states */
	enum class State : unsigned {Active, Preparing, Committed, Arborted};
	State state;

	Transaction():Tid(GMI_tid++),begin(getTimestamp()), end(1ull<<63), state(State::Active){

	}


	void setState(State state){
		this->state = state;
	}

	void decreaseCommitDepCounter(){
		//i ? ++CommitDepCounter:--CommitDepCounter;
		--CommitDepCounter;
	}

	void execute();

	void lookup(string, Predicate);

	void commit();

	void arbort();

};


//struct TransactionManager{
//	unordered_map<uint64_t, Transaction> transactions;
//};


#endif /* SRC_TRANSACTION_HPP_ */
