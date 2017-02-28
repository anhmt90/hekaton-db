/*
 * ImportDB.cpp
 *
 *  Created on: Oct 24, 2016
 *      Author: anhmt90
 */
#ifndef H_TPCC_SCHEMA
#define H_TPCC_SCHEMA

#include <fstream>
#include <sstream>
#include <iostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <map>
#include <utility>
#include <vector>
#include <algorithm>
#include <chrono>
#include <atomic>

#include "Types.hpp"
#include "Table.hpp"

using namespace std;
using namespace std::chrono;

typedef tuple<Integer, Integer> tup_2Int;
typedef tuple<Integer, Integer, Integer> tup_3Int;
typedef tuple<Integer, Integer, Integer, Integer> tup_4Int;



/*---------------------------------------------------------Global Variables/Function----------------------------------------------------------*/
//Global, monotonically increasing counter
extern atomic<uint64_t> GMI_cnt;
//Represent infinite timestamp
extern const uint64_t INF;
//get Timestampe from GMI_cnt and increase it by 1
extern uint64_t getTimestamp();
/*---------------------------------------------------------Supporting functions------------------------------------------------------*/

void close_ifstream(ifstream& itbl);
vector<string> split(const string &s);


/*-----------------------------------------------------------------------------------------------------------------------*/
namespace std {

template<>
struct hash<Integer> {
	size_t operator()(Integer const& key) const {
		size_t hash = 0;
		hash = hashKey<Integer>(key);
		return hash;
	}
};

template<>
struct hash<tuple<Integer>> {
	size_t operator()(tuple<Integer> const& key) const {
		size_t hash = 0;
		hash = hashKey<Integer>(get<0>(key));
		return hash;
	}
};

template<>
struct hash<tuple<Integer, Integer>> {
	size_t operator()(tuple<Integer, Integer> const& key) const {
		size_t hash = 0;
		hash = hashKey<Integer, Integer>(get<0>(key), get<1>(key));
		return hash;
	}
};

template<>
struct hash<tuple<Integer, Integer, Integer>> {
	size_t operator()(tuple<Integer, Integer, Integer> const& key) const {
		size_t hash = 0;
		hash = hashKey<Integer, Integer, Integer>(get<0>(key), get<1>(key),
				get<2>(key));
		return hash;
	}
};

template<>
struct hash<tuple<Integer, Integer, Integer, Integer>> {
	size_t operator()(
			tuple<Integer, Integer, Integer, Integer> const& key) const {
		size_t hash = 0;
		hash = hashKey<Integer, Integer, Integer, Integer>(get<0>(key),
				get<1>(key), get<2>(key), get<3>(key));
		return hash;
	}
};

template<>
struct hash<tuple<Integer, Integer, Varchar<16>, Varchar<16>>> {
	size_t operator()(
			tuple<Integer, Integer, Varchar<16>, Varchar<16>> const& key) const {
		size_t hash = 0;
		hash = hashKey<Integer, Integer, Varchar<16>, Varchar<16>>(get<0>(key),
				get<1>(key), get<2>(key), get<3>(key));
		return hash;
	}
};
}

//template<>
//struct hash<tuple<Integer, Integer, Varchar<16>, Varchar<16>>> {
//	size_t operator()( tuple<Integer, Integer, Varchar<16>, Varchar<16>> const& key) const {
//		size_t hash = 0;
//		hash = hashKey<Integer, Integer, Varchar<16>, Varchar<16>>(get<0>(key),
//				get<1>(key), get<2>(key), get<3>(key));
//		return hash;
//	}
//};

/*--------------------------------Define structure of a row for each table-----------------------------------*/

struct Version {
	uint64_t begin = 0;
	uint64_t end = 0;
	// Version* next;

	bool isGarbage = false;

	virtual void vf(){ };

	void setBegin(uint64_t begin){
		this->begin = begin;
	}

	void setEnd(uint64_t end){
		this->end = end;
	}

	Version(){ }
	virtual ~Version(){ };
};

struct Warehouse : public Table{
	struct Tuple : public Version{
		Integer w_id;
		Varchar<10> w_name;
		Varchar<20> w_street_1, w_street_2, w_city;
		Char<2> w_state;
		Char<9> w_zip;
		Numeric<4, 4> w_tax;
		Numeric<12, 2> w_ytd;

		Tuple(){ };
		Tuple(uint64_t begin){
			this->begin = begin;
		}

		Tuple(
				Integer id,
				Varchar<10> name,
				Varchar<20> street_1,
				Varchar<20> street_2,
				Varchar<20> city,
				Char<2> state,
				Char<9> zip,
				Numeric<4, 4> tax,
				Numeric<12, 2> ytd
		):
			w_id(id), w_name(name), w_street_1(street_1), w_street_2(street_2), w_city(city),
			w_state(state), w_zip(zip), w_tax(tax), w_ytd(ytd){ }
	};

	unordered_multimap<Integer, Warehouse::Tuple> pk_index;

	Warehouse(){
		this->name = "warehouse";
		tables.push_back(*this);
		import();
	}
	virtual ~Warehouse(){};
	void import();


};





struct District : public Table{
	struct Tuple : public Version{
		Integer d_id, d_w_id; //PKey
		Varchar<10> d_name;
		Varchar<20> d_street_1, d_street_2, d_city;
		Char<2> d_state;
		Char<9> d_zip;
		Numeric<4, 4> d_tax;
		Numeric<12, 2> d_ytd;
		Integer d_next_o_id;

		Tuple(){}

		Tuple(
				Integer id,
				Integer w_id,
				Varchar<10> name,
				Varchar<20> street_1,
				Varchar<20> street_2,
				Varchar<20> city,
				Char<2> state,
				Char<9> zip,
				Numeric<4, 4> tax,
				Numeric<12, 2> ytd,
				Integer next_o_id
		):
			d_id(id), d_w_id(w_id),
			d_name(name), d_street_1(street_1), d_street_2(street_2), d_city(city),
			d_state(state), d_zip(zip), d_tax(tax), d_ytd(ytd),
			d_next_o_id(next_o_id){ }

	};

	unordered_multimap<tup_2Int, District::Tuple> pk_index;

	District(){
		this->name = "district";
		tables.push_back(*this);
		import();
	}
	virtual ~District(){};
	void import();
};





struct Customer : public Table{
	struct Tuple : public Version {
		Integer c_id, c_d_id, c_w_id; //Pkey
		Varchar<16> c_first;
		Char<2> c_middle;
		Varchar<16> c_last;
		Varchar<20> c_street_1, c_street_2, c_city;
		Char<2>  c_state;
		Char<9> c_zip;
		Char<16> c_phone;
		Timestamp c_since;
		Char<2> c_credit;
		Numeric<12, 2> c_credit_lim;
		Numeric<4, 4> c_discount;
		Numeric<12, 2> c_balance,c_ytd_payment;
		Numeric<4, 0> c_payment_cnt, c_delivery_cnt;
		Varchar<500> c_data;

		Tuple(){}

		Tuple(
				Integer id,
				Integer d_id,
				Integer w_id,
				Varchar<16> first,
				Char<2> middle,
				Varchar<16> last,
				Varchar<20> street_1,
				Varchar<20> street_2,
				Varchar<20> city,
				Char<2>  state,
				Char<9> zip,
				Char<16> phone,
				Timestamp since,
				Char<2> credit,
				Numeric<12, 2> credit_lim,
				Numeric<4, 4> discount,
				Numeric<12, 2> balance,
				Numeric<12, 2> ytd_payment,
				Numeric<4, 0> payment_cnt,
				Numeric<4, 0> delivery_cnt,
				Varchar<500> data
		):
			c_id(id), c_d_id(d_id), c_w_id(w_id),
			c_first(first), c_middle(middle), c_last(last),
			c_street_1(street_1), c_street_2(street_2), c_city(city),
			c_state(state), c_zip(zip), c_phone(phone), c_since(since),
			c_credit(credit),c_credit_lim(credit_lim), c_discount(discount),
			c_balance(balance), c_ytd_payment(ytd_payment),
			c_payment_cnt(payment_cnt), c_delivery_cnt(delivery_cnt), c_data(data){ }
	};
	unordered_multimap<tup_3Int, Customer::Tuple> pk_index;

	Customer(){
		this->name = "customer";
		tables.push_back(*this);
		import();
	}
	virtual ~Customer(){};
	void import();
};





struct History : public Table{
	struct Tuple : public Version {
		Integer h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id;
		Timestamp h_date;
		Numeric<6, 2> h_amount;
		Varchar<24> h_data;

		Tuple(){}

		Tuple(	Integer c_id,
				Integer c_d_id,
				Integer c_w_id,
				Integer d_id,
				Integer w_id,
				Timestamp date,
				Numeric<6, 2> amount,
				Varchar<24> data
		):
			h_c_id(c_id), h_c_d_id(c_d_id), h_c_w_id(c_w_id),
			h_d_id(d_id), h_w_id(w_id), h_date(date),
			h_amount(amount), h_data(data){}
	};

	History(){
		this->name = "history";
		tables.push_back(*this);
		import();
	}
	virtual ~History(){};
	void import();
};






struct NewOrder : public Table{
	struct Tuple : public Version {
		Integer no_o_id, no_d_id, no_w_id; //Pkey

		Tuple(){}

		Tuple(Integer o_id, Integer d_id, Integer w_id):
			no_o_id(o_id), no_d_id(d_id), no_w_id(w_id){

		}
	};

	unordered_multimap<tup_3Int, NewOrder::Tuple> pk_index;

	NewOrder(){};
	NewOrder(string name){
		this->name = name;
		tables.push_back(*this);
		import();
	}
	virtual ~NewOrder(){};
	void import();
};






struct Order : public Table{
	struct Tuple : public Version{
		Integer o_id, o_d_id, o_w_id; //Pkey
		Integer o_c_id;
		Timestamp o_entry_d;
		Integer o_carrier_id;
		Numeric<2, 0> o_ol_cnt;
		Numeric<1, 0> o_all_local;

		Tuple(){}

		Tuple(
				Integer id,
				Integer d_id,
				Integer w_id,
				Integer c_id,
				Timestamp entry_d,
				Integer carrier_id,
				Numeric<2,0> ol_cnt,
				Numeric<1, 0> all_local
			):
					o_id(id), o_d_id(d_id), o_w_id(w_id),
					o_c_id(c_id), o_entry_d(entry_d), o_carrier_id(carrier_id),
					o_ol_cnt(ol_cnt), o_all_local(all_local){

		}
	};

	unordered_multimap<tup_3Int, Order::Tuple> pk_index;

	Order(){};
	Order(string name){
		this->name = name;
		tables.push_back(*this);
		import();
	}
	virtual ~Order(){};
	void import();
};






struct OrderLine : public Table{
	struct Tuple : public Version {
		Integer ol_o_id, ol_d_id, ol_w_id, ol_number; //Pkey
		Integer ol_i_id, ol_supply_w_id;
		Timestamp ol_delivery_d;
		Numeric<2, 0> ol_quantity;
		Numeric<6, 2> ol_amount;
		Char<24> ol_dist_info;

		Tuple(uint64_t begin){
			this->begin = begin;
		}

		Tuple(
				Integer o_id,
				Integer d_id,
				Integer w_id,
				Integer number,
				Integer i_id,
				Integer supply_w_id,
				Timestamp delivery_d,
				Numeric<2, 0> quantity,
				Numeric<6, 2> amount,
				Char<24> dist_info
				):
					ol_o_id(o_id), ol_d_id(d_id), ol_w_id(w_id),
					ol_number(number), ol_i_id(i_id),
					ol_supply_w_id(supply_w_id), ol_delivery_d(delivery_d),
					ol_quantity(quantity), ol_amount(amount),
					ol_dist_info(dist_info){

		}
	};

	unordered_multimap<tup_4Int, OrderLine::Tuple> pk_index;

	OrderLine(){
		this->name = "orderline";
		tables.push_back(*this);
		import();
	}
	virtual ~OrderLine(){};
	void import();
};

struct Item : public Table{
	struct Tuple : public Version {
		Integer i_id;	//Pkey
		Integer i_im_id;
		Varchar<24> i_name;
		Numeric<5, 2> i_price;
		Varchar<50> i_data;


		Tuple(){}

		Tuple(
				Integer id,
				Integer im_id,
				Varchar<24> name,
				Numeric<5, 2> price,
				Varchar<50> data
		):
				i_id(id),
				i_im_id(im_id), i_name(name), i_price(price),
				i_data(data){ }
	};
	unordered_multimap<Integer, Item::Tuple> pk_index;

	Item(){};
	Item(string name){
		this->name = name;
		tables.push_back(*this);
		import();
	}
	virtual ~Item(){};
	void import();

};

struct Stock : public Table{
	struct Tuple : public Version{
		Integer s_i_id, s_w_id; //Pkey
		Numeric<4, 0> s_quantity;
		Char<24> s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
		s_dist_07, s_dist_08, s_dist_09, s_dist_10;
		Numeric<8, 0> s_ytd;
		Numeric<4, 0> s_order_cnt, s_remote_cnt;
		Varchar<50> s_data;

		Tuple(){}

		Tuple(
				Integer i_id,
				Integer w_id,
				Numeric<4, 0> quantity,
				Char<24> dist_01, Char<24> dist_02, Char<24> dist_03, Char<24> dist_04,
				Char<24> dist_05, Char<24> dist_06, Char<24> dist_07, Char<24> dist_08,
				Char<24> dist_09, Char<24> dist_10,
				Numeric<8, 0> ytd,
				Numeric<4, 0> order_cnt,
				Numeric<4, 0> remote_cnt,
				Varchar<50> data
		):
			s_i_id(i_id), s_w_id(w_id),
			s_quantity(quantity), s_dist_01(dist_01), s_dist_02(dist_02), s_dist_03(dist_03),
			s_dist_04(dist_04),s_dist_05(dist_05), s_dist_06(dist_06),
			s_dist_07(dist_07), s_dist_08(dist_08),s_dist_09(dist_09), s_dist_10(dist_10),
			s_ytd(ytd), s_order_cnt(order_cnt), s_remote_cnt(remote_cnt), s_data(data){ }
	};

	unordered_multimap<tup_2Int, Stock::Tuple> pk_index;

	Stock(){};
	Stock(string name){
		this->name = name;
		tables.push_back(*this);
		import();
	}
	virtual ~Stock(){};
	void import();
};




extern Warehouse warehouse;
extern District district;
extern Customer customer;
extern NewOrder neworder;
extern Order order;
extern OrderLine orderline;
extern Item item;
extern Stock stock;



typedef unordered_multimap<Integer, Warehouse::Tuple> Warehouse_PK;
typedef unordered_multimap<tup_2Int, District::Tuple> District_PK;
typedef unordered_multimap<tup_3Int, Customer::Tuple> Customer_PK;
typedef unordered_multimap<tup_3Int, NewOrder::Tuple> NewOrder_PK;
typedef unordered_multimap<tup_3Int, Order::Tuple> Order_PK;
typedef unordered_multimap<tup_4Int, OrderLine::Tuple> OrderLine_PK;
typedef unordered_multimap<Integer, Item::Tuple> Item_PK;
typedef unordered_multimap<tup_2Int, Stock::Tuple> Stock_PK;








/*-----------------------------------------------------------------------------------------------------------------------*/



/*
 * Primary key indexes of each table as unordered_map
 * with key part is the primary key and value part is
 * index-number on the vector of the table.
 */
//extern Warehouse warehouse;
//extern OrderLine orderline;
//	unordered_map<tuple<Integer, Integer>, District_Tuple> district;
//	unordered_map<tuple<Integer, Integer, Integer>, Customer_Tuple> customer;
//	unordered_map<tuple<Integer, Integer, Integer>, NewOrder_Tuple> neworder;
//	unordered_map<tuple<Integer, Integer, Integer>, Order_Tuple> order;
//	unordered_map<tuple<Integer, Integer, Integer, Integer>, OrderLine_Version> orderline;
//	unordered_map<Integer, Item_Tuple> item;
//	unordered_map<tuple<Integer, Integer>, Stock_Tuple> stock;


void _import();
//inline void Warehouse_Import(ifstream& );
//
//inline void OrderLine_Import(ifstream&);

struct TPCC {

	TPCC();

	~TPCC();

//	void Warehouse_Insert(Integer , Warehouse_Tuple &);

//	void Warehouse_Import(ifstream& );
//	/*-----------------------------------------------------------------------------------------------------------------------*/
//	void District_Insert(tup_2Int, District_Tuple&);
//
//	void District_Import(ifstream&);
//	/*-----------------------------------------------------------------------------------------------------------------------*/
//	void Customer_Insert(tup_3Int, Customer_Tuple&);
//
//	inline void Customer_Import(ifstream&);
//	/*----------------------------------------------------------------------------------------------------------------------*/
//	void History_Insert(History_Tuple&);
//
//	void History_Import(ifstream&);
//	/*-----------------------------------------------------------------------------------------------------------------------*/
//	void NewOrder_Insert(tup_3Int, NewOrder_Tuple);
//
//	inline void NewOrder_Import(ifstream&);
//	/*-----------------------------------------------------------------------------------------------------------------------*/
//	void Order_Insert(tup_3Int, Order_Tuple&);
//
//	inline void Order_Import(ifstream&);
//	/*-----------------------------------------------------------------------------------------------------------------------*/
//	void OrderLine_Insert(tup_4Int, OrderLine_Tuple);
//
//	inline void OrderLine_Import(ifstream&);
//	/*-----------------------------------------------------------------------------------------------------------------------*/
//	void Item_Insert(Integer, Item_Tuple&);
//
//	void Item_Import(ifstream&);
//	/*-----------------------------------------------------------------------------------------------------------------------*/
//	void Stock_Insert(tup_2Int, Stock_Tuple&);
//
//	void Stock_Import(ifstream&);

	/*-----------------------------------------------------------------------------------------------------------------------*/
	//std::ostream& operator<<(std::ostream& out,const w_Tuple& value);
	//For the two indexes

//	void _importIndex();
//	void _import();


};
#endif
