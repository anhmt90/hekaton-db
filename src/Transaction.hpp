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

using namespace std;

extern uint64_t GMI_tid;

struct Transaction{
	const uint64_t Tid;
	uint64_t begin;
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
	set<uint64_t> CommitDepSet;

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
};


//struct TransactionManager{
//	unordered_map<uint64_t, Transaction> transactions;
//};


#endif /* SRC_TRANSACTION_HPP_ */
