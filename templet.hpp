#include <functional>
#include <sstream>
#include <map>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>

namespace templet {

	class wal {
	public:
		virtual void write(unsigned& index, unsigned tag, const std::string& blob){
            std::unique_lock<std::mutex> lock(mut);
			log.push_back(std::pair<unsigned, std::string>(tag, blob));
			index = log.size() - 1;
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
		globj(wal&l) :_wal(l), wal_index(0), is_init(false) {}
		void init() { is_init = true; on_init(); is_init = false; }

		void update() {
			unsigned tag; std::string blob;
			for (; _wal.read(wal_index, tag, blob); wal_index++) {
				changer& changer = changers[tag];
				if (changer.use_input) { std::istringstream in(blob); changer.on_update_input(in); }
				else changer.on_update();
			}
		}
		void update(const unsigned id,
			std::function<void(void)>update) {
			if (is_init) 
				changers[id] = changer(update);
			else {
				std::string empty; unsigned index;
				_wal.write(index, id, empty);
				globj::update(index);
			}
		}
		void update(const unsigned id,
			std::function<void(std::ostream&)>save,
			std::function<void(std::istream&)>update) {
			if (is_init)
				changers[id] = changer(update);
			else {
				std::ostringstream out; unsigned index;
				save(out); _wal.write(index, id, out.str());
				globj::update(index);
			}
		}
	protected:
		virtual void on_init() = 0;
	private:
		void update(unsigned index) {
			unsigned tag; std::string blob;
			for (; wal_index <= index && _wal.read(wal_index, tag, blob); wal_index++) {
				changer& changer = changers[tag];
				if (changer.use_input) { std::istringstream in(blob); changer.on_update_input(in); }
				else changer.on_update();
			}
		}
		struct changer {
			changer(std::function<void(std::istream&)>update) :on_update_input(update), use_input(true){}
			changer(std::function<void(void)>update) :on_update(update), use_input(false) {}
			changer() : use_input(false), on_update_input([](std::istream&){}), on_update([](){}){}
			bool use_input;
			std::function<void(std::istream&)>on_update_input;
			std::function<void(void)>on_update;
		};
		wal& _wal;
		unsigned wal_index;
		std::map<unsigned,changer> changers;
		bool is_init;
	};

    class job {
	public:
		job(unsigned size):_size(size),_PID(0){}

		void run(std::function<void(unsigned pid)> process){
            std::vector<std::thread> threads(_size);
            _beg=std::chrono::high_resolution_clock::now();
        	for (auto& t : threads) t = std::thread([&]{process(_PID++);}); 
            for (auto& t : threads) t.join();
            _end=std::chrono::high_resolution_clock::now();
        }
		void delay(double seconds){
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
