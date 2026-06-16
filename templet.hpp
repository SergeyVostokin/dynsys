#include <functional>
#include <sstream>
#include <map>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>
#include <vector>
#include <cassert>
#include <iostream>

#if (__cplusplus>=201703L)
#include <filesystem>
#endif

namespace templet {
    class wal {
	public:
		virtual void write(unsigned& index, unsigned tag, const std::string& blob) = 0;
		virtual bool read(unsigned index, unsigned& tag, std::string& blob) = 0;
	};

	class filewal :public wal {
	public:
		filewal(const char filename[], bool lazy = true) :filewal(std::string(filename), lazy) {}
		filewal(const std::string& filename, bool lazy = true) :
			_current_index(0), _cashed_write(false), _filename(filename), _lazy(lazy) {
			_file = fopen(_filename.c_str(), "rb");
			if (!_file) {
				_file = fopen(_filename.c_str(), "ab");
				assert(_file && "filewal: cannot open log file");
				_initial_read = false;
			}
			else
				_initial_read = true;
		}
		~filewal() { fclose(_file); }

		void write(unsigned& index, unsigned tag, const std::string& blob) override {
			assert(!_initial_read && "filewal: access pattern violated");

			size_t ret_code;
			unsigned ubuf[3];//index,tag,blob size

			ubuf[0] = _current_index; //index
			ubuf[1] = tag;   //tag
			ubuf[2] = blob.size(); //blob size

			ret_code = fwrite(ubuf, sizeof(ubuf), 1, _file);
			assert(ret_code == 1 && "filewal: write error");

			ret_code = fwrite(blob.c_str(), sizeof(char), ubuf[2], _file);
			assert(ret_code == ubuf[2] && "filewal: write error");

			if (!_lazy) fflush(_file);

			_cashed_tag = tag; _cashed_blob = blob; _cashed_write = true;			
            index=_current_index; _current_index++;
		}
		bool read(unsigned index, unsigned& tag, std::string& blob) override {
			if (_initial_read) {
                assert(index == _current_index && "filewal: access pattern violated");
                
				size_t ret_code;
				unsigned ubuf[3];//index,tag,blob size

				ret_code = fread(ubuf, 1, sizeof(ubuf), _file);

				if (ret_code == 0 && feof(_file)) {
					fclose(_file);
					_file = fopen(_filename.c_str(), "ab");
					assert(_file && "filewal: cannot open log file");
					_initial_read = false;
					return false;
				}                
                
				assert(ret_code == sizeof(ubuf) && "filewal: read error");
				assert(ubuf[0] == _current_index && "filewal: integrity is compromised");         

				tag = ubuf[1];//tag
				blob.resize(ubuf[2]);//size
                
				ret_code = fread((void*)blob.c_str(), sizeof(char), ubuf[2], _file);//blob
                assert(ret_code == ubuf[2] && "filewal: read error");

				_current_index++;
				return true;
			}
			else {// !_initial_read (write)
				assert((index == _current_index || index == _current_index-1)
                    && "filewal: access pattern violated");

				if (index == _current_index-1 && _cashed_write) {
					tag = _cashed_tag; blob = _cashed_blob;
					return true;
				}
				return false;
			}
		}
	private:
		FILE*_file;
		std::string _filename;
		bool _initial_read;
		unsigned _current_index;
		bool _cashed_write;
		unsigned _cashed_tag;
		std::string _cashed_blob;
		bool _lazy;
	private:
		void truncate_chunk(const std::string& filename, unsigned n) {
#if (__cplusplus>=201703L)
			std::filesystem::path p = filename;
			std::filesystem::resize_file(p, std::filesystem::file_size(p) - n);
#else
			assert(!"filewal: the last entry is corrupted");
#endif
		}
	};

	class memwal :public wal {
	public:
		void write(unsigned& index, unsigned tag, const std::string& blob) override {
			std::unique_lock<std::mutex> lock(_mut);
			_log.push_back(std::pair<unsigned, std::string>(tag, blob));
			index = (unsigned)(_log.size() - 1);
		}
		bool read(unsigned index, unsigned& tag, std::string& blob) override {
			std::unique_lock<std::mutex> lock(_mut);
			if (index < _log.size()) { tag = _log[index].first; blob = _log[index].second; return true; }
			return false;
		}
		void print(std::ostream& out) {
			for (int i = 0; i < _log.size(); i++) {
				out << "index:" << i << " tag:" << _log[i].first << std::endl
					<< "entry:" << _log[i].second << std::endl;
			}
		}
	protected:
		std::vector<std::pair<unsigned, std::string>> _log;
		std::mutex _mut;
	};

	class globj {
	public:
		globj(wal&w):_wal(w), _wal_index(0), _is_init(false) {}
		void init() { _is_init = true; on_init(); _is_init = false; update(); }
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
				std::unique_lock<std::mutex> lock(_mut);

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
			std::unique_lock<std::mutex> lock(_mut);

			unsigned tag; std::string blob;
			std::ostringstream out;

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
		std::mutex _mut;
	};

    class job {
	public:
		job(unsigned size):_size(size),_taskID(0){}
        job():_size(std::thread::hardware_concurrency()),_taskID(0){}
        void init(unsigned size){_size = size;}
    public:
        inline void operator()(std::function<void(unsigned taskID)> task){
            job::run(task);
        }
		void run(std::function<void(unsigned taskID)> task){
            std::vector<std::thread> threads(_size);
            _beg=std::chrono::high_resolution_clock::now();
        	for (auto& t : threads) t = std::thread([&]{task(_taskID++);}); 
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
        std::atomic_int _taskID = 0;
        std::chrono::time_point<std::chrono::high_resolution_clock> _beg, _end;
	};
}
