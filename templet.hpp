#include <functional>
#include <sstream>
#include <map>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>
#include <vector>

namespace templet {

	class wal {
	public:
		virtual void write(unsigned& index, unsigned tag, const std::string& blob){
            std::unique_lock<std::mutex> lock(mut);
			log.push_back(std::pair<unsigned, std::string>(tag, blob));
			index = (unsigned)(log.size() - 1);
        }
		virtual bool read(unsigned index, unsigned& tag, std::string& blob){
            std::unique_lock<std::mutex> lock(mut);
			if (index < log.size()) { tag = log[index].first; blob = log[index].second; return true; }
			return false;
        }
	protected:
		std::vector<std::pair<unsigned, std::string>> log;
		std::mutex mut;
	};

	class globj {
	public:
		globj(wal&w):_wal(w), _wal_index(0), _is_init(false) {}
		void init() { _is_init = true; on_init(); _is_init = false; }
	protected:
		virtual void on_init() = 0;
	public:
		void update(
			unsigned id,
			std::function<void(std::ostream&)> save,
			std::function<void(std::istream&, std::ostream&)> update,
			std::function<void(std::istream&)> load = [](std::istream&) {}
		) {
			if (_is_init)
				_updaters[id] = update;
			else {
				std::ostringstream out; unsigned index;
				save(out); _wal.write(index, id, out.str()); out.clear();

				unsigned tag; std::string blob;
				for (; _wal_index < index && _wal.read(_wal_index, tag, blob); _wal_index++) {
					auto& updater = _updaters[tag];
					{
						std::istringstream in(blob);
						updater(in, out); out.clear();
					}
				}
				_wal.read(_wal_index, tag, blob); _wal_index++;
				{
					auto& updater = _updaters[tag];
					std::istringstream in(blob); std::stringstream out;
					updater(in, out); load(out);
				}
			}
		}
		inline void update(
			unsigned id,
			std::function<void(std::istream&, std::ostream&)> update,
			std::function<void(std::istream&)> load = [](std::istream&) {}
		) {
			globj::update(id, [](std::ostream&) {}, update, load);
		}
		void update() {
			unsigned tag; std::string blob; std::ostringstream out;
			for (; _wal.read(_wal_index, tag, blob); _wal_index++) {
				auto& updater = _updaters[tag];
				{ 
					std::istringstream in(blob); 
					updater(in, out); out.clear();
				}
			}
		}
	private:
		wal& _wal;
		unsigned _wal_index;
		bool _is_init;
		std::map<unsigned,std::function<void(std::istream&, std::ostream&)>> _updaters;
	};

    class map {
	public:
		map(wal&){}
		void init(unsigned size){}
        inline void operator()(
			std::function<void(unsigned size)> init,
			std::function<void(unsigned iter)> map,
			std::function<void(unsigned iter, std::ostream&, bool mapped)> save
			= [](unsigned, std::ostream&, bool) {},
			std::function<void(unsigned iter, std::istream&, bool mapped)> load
			= [](unsigned, std::istream&, bool) {}
        ){ map::run(init,map,save,load); }
		void run(
			std::function<void(unsigned size)> init,
			std::function<void(unsigned iter)> map,
			std::function<void(unsigned iter, std::ostream&, bool mapped)> save
			= [](unsigned, std::ostream&, bool) {},
			std::function<void(unsigned iter, std::istream&, bool mapped)> load
			= [](unsigned, std::istream&, bool) {}
		){}
	};

    class async {
	public:
		async(wal&){}
        inline void task(
			std::function<void(std::ostream&)> action,
			std::function<void(std::istream&)> load
		){ async::task(false,action,load); }
		void task(bool local,
			std::function<void(std::ostream&)> action,
			std::function<void(std::istream&)> load
		){}
		void wait(){}
	};

    class acta {
	public:
		acta(wal&){}
		void run(){}
		inline void operator()() { acta::run(); }
		virtual void on_run() {}
	public:
		class actor;
		class message {
		public:
            message(){}
			message(actor&){}
			message(actor&, std::function<void()> action){}
			message(actor*){}
			message(actor*, std::function<void()> action){}
            void init(actor&){}
            void init(actor&, std::function<void()> action){}
            void init(actor*){}
            void init(actor*, std::function<void()> action){}
		public:
			bool operator()(){return false;};// is available now?
			void cli(actor*,std::function<void()> action){}
			void srv(actor*,std::function<void()> action){}
            void cli(actor&,std::function<void()> action){}
			void srv(actor&,std::function<void()> action){}
		};
	public:
		class actor {
		public:
            actor(){}
			actor(acta&,bool start=false){}
			actor(acta*,bool start = false){}
            void init(acta&,bool start = false){}
            void init(acta*,bool start = false){}
		public:
			void ask(message&){}
			void ask(message*){}
			void ret(message&){}
			void ret(message*){}
			void say(message&){}
			void say(message*){}
		public:
			void task(bool local,
				std::function<void(std::ostream&)> action,
				std::function<void(std::istream&)> load
			){}
			inline void task(
				std::function<void(std::ostream&)> action,
				std::function<void(std::istream&)> load
			) {
				actor::task(false,action,load);
			}
			void stop(){}
        protected:
            virtual void on_start(){}
		};
	};

    class job {
	public:
		job(unsigned size):_size(size),_PID(0){}
        job():_size(0),_PID(0){}
        void init(unsigned size){_size = size;}
    public:
        inline void operator()(std::function<void(unsigned pid)> process){
            job::run(process);
        }
		void run(std::function<void(unsigned pid)> process){
            std::vector<std::thread> threads(_size);
            _beg=std::chrono::high_resolution_clock::now();
        	for (auto& t : threads) t = std::thread([&]{process(_PID++);}); 
            for (auto& t : threads) t.join();
            _end=std::chrono::high_resolution_clock::now();
        }
		static void delay(double seconds){
            std::this_thread::sleep_for(
                std::chrono::duration<double>(seconds));
        }
		double duration(){ 
            std::chrono::duration<double> dur = _end - _beg;
            return dur.count();
        }
    private:
        unsigned _size;
        std::atomic_int _PID = 0;
        std::chrono::time_point<std::chrono::high_resolution_clock> _beg, _end;
	};
}
