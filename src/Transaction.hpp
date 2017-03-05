/*
 * Transaction.hpp
 *
 *  Created on: Jan 23, 2017
 *      Author: anhmt90
 */
/*
 * Abort CODE:
 * 				1 = commit succeeds
 * 				-1 = Visibility validation failed
 * 				-2 = Phantom validation failed
 * 				-19 = cascaded abort by commit dependency
 *
 */

#ifndef SRC_TRANSACTION_HPP_
#define SRC_TRANSACTION_HPP_

#include <climits>	//for CHAR_BIT
#include <set>
#include <cstdint>
#include <atomic>
#include <thread>

#include "Schema.hpp"

using namespace std;


/****************************************************************************/
struct Transaction;

// value to present a null value of a field
const uint64_t NOT_SET = 1ull<<63;

// counter to generate the Transactions ID
extern atomic<uint64_t> GMI_tid;

// container to manage all running Transactions
extern unordered_map<uint64_t,Transaction*> TransactionManager;

// vector to store terminated Transactions
extern vector<Transaction*> GarbageTransactions;

/****************************************************************************/
uint64_t getTid();
/****************************************************************************/

class TransactionNotFoundException : public exception {
public:
	TransactionNotFoundException(){}
};



struct Predicate {
	Integer pk_int;
	tup_2Int pk_2int;
	tup_3Int pk_3int;
	tup_4Int pk_4int;

	Predicate(Integer pk_int) : pk_int(pk_int){

	}

	Predicate(Integer key1, Integer key2):pk_2int(make_tuple(key1, key2)){
	}

	Predicate(Integer key1, Integer key2, Integer key3) : pk_3int(make_tuple(key1, key2, key3)){

	}

	Predicate(Integer key1, Integer key2, Integer key3, Integer key4) : pk_4int(make_tuple(key1, key2, key3, key4)){

	}

	~Predicate(){ };

	template<typename H, typename... T> void toString(H head, T... tail){
		cout << head << ", ";
		toString(tail...);
	}
};




struct Transaction{
	// Transaction ID
	const uint64_t Tid;
	// begin time of the transaction, also used as logical read time
	const uint64_t begin;
	/*
	 *  end time of the transaction, will be set as INF in the constructor,
	 *  will be set by getTimestamp() again when the transaction precommits.
	 */
	uint64_t end;

	/*
	 * counts how many unresolved commit dependencies this transaction still has.
	 * A transaction cannot commit until this counter = 0
	 */
	int CommitDepCounter = 0;

	/*
	 * other transactions can set this variable to tell this transaction
	 * to abort.
	 */
	bool AbortNow = false;

	//terminate code
	int CODE;

	/* each transaction can be in 1 of 4 states */
	enum class State : unsigned {Active, Preparing, Committed, Aborted};
	State state;

	/*
	 * stores Tid of the transactions that depend on this transaction
	 */
	vector<uint64_t> CommitDepSet;

	/*
	 *	stores the Versions read, not ones updated
	 */
	vector<Version*> ReadSet;

	vector<pair<Warehouse_PK*,Integer>> ScanSet_Warehouse;
	vector<pair<District_PK*,tup_2Int>> ScanSet_District;
	vector<pair<Customer_PK*,tup_3Int>> ScanSet_Customer;
	vector<pair<NewOrder_PK*,tup_3Int>> ScanSet_NewOrder;
	vector<pair<Order_PK*,tup_3Int>> ScanSet_Order;
	vector<pair<OrderLine_PK*,tup_4Int>> ScanSet_OrderLine;
	vector<pair<Item_PK*,Integer>> ScanSet_Item;
	vector<pair<Stock_PK*,tup_2Int>> ScanSet_Stock;
	/*
	 * first: old version
	 * second: new version
	 * if second  = nullptr, then it is a delete
	 * if first = nullptr, then it is a insert
	 */
	vector<pair<Version*, Version*>> WriteSet;


	Transaction(int i) : Tid(getTid()),begin(getTimestamp()), end(NOT_SET), state(State::Active){
		TransactionManager.insert(make_pair(Tid, this));
		execute(i);
	}

	~Transaction(){ };

	int checkVisibility(Version& V);
	int checkUpdatibility(Version& V);

	void execute(int);

	Version* read(string tableName, Predicate pred, bool from_update);
	Version* update(string tableName, Predicate pred, bool del);
	Version* insert(string tableName, Version* VI, Predicate pred);

	int validate();
	void precommit();
	void abort(int code);
	void commit();

	//not so important function
	bool readable(Version* V, bool from_update); //for the sake of more concise code
	void decreaseCommitDepCounter();
};

#endif /* SRC_TRANSACTION_HPP_ */
