#include "templet.hpp"
#include <random>
#include <set>
#include <iostream>

class bag_of_tasks:public templet::globj{
public:
    bag_of_tasks(templet::wal&l):globj(l) { 
        init(); _rand.seed(std::time(nullptr));}
    void resize(unsigned size){
        update(_resize, [&](std::ostream&out) {
            out << size;
		},
		[this](std::istream&in) {
            unsigned size; in >> size;
            //----------------------------------------
            N.resize(size); NxN.resize(size);
            unprocessed.clear(); _ready_to_get = false;
            //----------------------------------------
		});   
    }
    void add(unsigned id,int n){
        update(_add, [&](std::ostream&out) {
            out << id << " " << n;
		},
		[this](std::istream&in) {
            unsigned id; int n; in >> id >> n;
            //----------------------------------------
            N[id]=n; unprocessed.insert(id);
            if(unprocessed.size()==N.size()) _ready_to_get = true;
            //----------------------------------------
		});     
    }
    bool ready_to_get(){
        update();
        //----------------------------------------
        return _ready_to_get;
        //----------------------------------------
    }
    bool get(unsigned& id, int& n){
        update();
        //----------------------------------------
        if(unprocessed.empty())return false;
        id = get_rand_unprocessed(); n = N[id];
        return true;
        //----------------------------------------
    }
    void put(unsigned id,int nxn){
        update(_put, [&](std::ostream&out) {
            out << id << " " << nxn;
		},
		[this](std::istream&in) {
            unsigned id; int nxn; in >> id >> nxn;
            //----------------------------------------
            unprocessed.erase(id);
            NxN[id]=nxn;
            //----------------------------------------
		});  
    }
    //----------------------------------------
public:
    std::vector<int> N;
    std::vector<int> NxN;
private:
    std::set<unsigned> unprocessed;
    bool _ready_to_get;
    std::minstd_rand _rand;
    unsigned get_rand_unprocessed(){
        int selected = _rand() % unprocessed.size();
		auto it = unprocessed.begin(); 
        for (int i = 0; i != selected; i++, it++) {}
        return *it;
    }
    //----------------------------------------
private:
	enum {_resize,_add,_put};
	void on_init() override {
		resize(0); add(0,0); put(0,0);
	}
};

int main()
{
    const int NUM_PROC = 1;
    const int SIZE = 10;

    templet::wal a_wal;
    templet::job a_job(SIZE);

//    a_job.run([&a_wal,&a_job](unsigned pid){
//        bag_of_tasks tbag(a_wal);

//        if(pid==0 && !tbag.ready_to_get()){// in master 'process'
//            tbag.resize(SIZE);
//            for(int i=0;i<SIZE;i++) tbag.add(i,i);
//        }
//        while(!tbag.ready_to_get())/*wait*/;
    
//        unsigned id; int N, NxN; 
//        while(tbag.get(id,N)){
//            NxN = N*N;
//            a_job.delay(1.0);//simulate a workload
//            tbag.put(id,NxN);
//        }
    
//        if(pid==0){// in master 'process'
//            for(int i=0;i<SIZE;i++)
//                std::cout << tbag.N[i] <<"^2 = " << tbag.NxN[i] << std::endl;
//        }             
//    });

    a_job.run([&a_job](unsigned pid){
        a_job.delay(1.0);
    });

    std::cout << "Duration with " << NUM_PROC << 
        " thread(s) is " << a_job.duration() << " seconds." << std::endl;

}