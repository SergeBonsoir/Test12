/**
Текст задания:
Необходимо написать программу, которая будет сортировать по возрастанию большой файл беззнаковых 32-х разрядных целых чисел. При этом:
- Размер входного файла значительно больше объема доступной оперативной памяти, которой есть всего 128 Мб.
- Числа в файле записаны в бинарном виде.
- Есть достаточно дискового пространства для хранения результата сортировки в отдельном файле и для хранения промежуточных результатов.
- Программа будет компилироваться при помощи g++-5.3.0 с опциями -std=c++14 -D_NDEBUG -O3 -lpthread.
- Файлы будут находиться на SSD диске. На компьютере стоит многоядерный процессор.
- Входной файл будет находиться в той же директории что и исполняемый файл и будет называться input. Мы ожидаем там же увидеть отсортированный файл с именем output.
- Из вашей программы должен получиться исполняемый файл, а значит нужно чтобы в ней была функция main.
- Никаких дополнительных библиотек на компьютере не установлено. (Например, нет boost).
- Решение должно быть кросс-платформенным.

Метод решения:
Использована оригинальная реализация алгоритма "сортировка слиянием" для нескольких потоков.
Определяется количество ядер процессора, для выполнения сортировки используются все.
Вид программы: консольное приложение.
Разработано в среде Windows с использованием MSVC 2019, проверено на Linux с компилятором g++-5.3.0.
**/
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <iostream>
#include <thread>
#include <vector>
#include <list>
#include <queue>
#include <sstream>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <exception>

#ifdef _MSC_VER
#define fseeko64 _fseeki64
#define ftello64 _ftelli64
#define off64_t int64_t
#endif

/** define data type **/
typedef unsigned int target_type;
constexpr const char* target_name = "UINT";

class tmpf;
std::list<tmpf*> tmpFiles;	// list store tmp file wrappers
std::mutex mtx;
std::mutex omtx;

/** input stream wrapper, for each thread **/
class chunk
{
public:
	chunk(unsigned int id, int64_t offset, int64_t sz, int64_t mem)
		: id_(id), offset_(offset), sz_(sz), mem_(mem), buf_(nullptr), state_(0) {}

	~chunk() { if (buf_) delete[] buf_; }

	unsigned int id_;
	int64_t offset_;
	int64_t sz_;
	int64_t mem_;
	target_type* buf_;
	int state_;
private:
	chunk() = delete;
};

/** tmp file wrapper **/
class tmpf
{
public:
	tmpf(std::string filename) : filename_(filename), sz_(0), f_(nullptr), buf_(nullptr), ptr_(nullptr), count_(0) {}
	~tmpf() 
	{
		if (f_) { fclose(f_); f_ = nullptr; } 
		if (buf_) { delete[] buf_; buf_ = nullptr; }
	}

	bool operator<(const tmpf& t);

	std::string filename_;
	FILE* f_;
	bool init(size_t sz);
	bool next();
	const target_type value() const { return ptr_ ? *ptr_ : 0; }

private:
	size_t count_;
	size_t sz_;
	target_type* buf_;
	target_type* ptr_;
	tmpf() {};
};

bool tmpf::operator<(const tmpf& t) 
{ 
	return (ptr_ && t.ptr_) ? *ptr_ < *t.ptr_ : false; 
}

bool tmpf::init(size_t sz)
{
	sz_ = sz;
	f_ = fopen(filename_.c_str(), "rb");
	buf_ = new target_type[sz_];
	return (f_ != nullptr);
};

/** move ptr_ to next value; fill buffer if need **/
 bool tmpf::next() 
{
	 if (ptr_) ++ptr_;
	 if (ptr_ == nullptr || ptr_ >= buf_ + count_)
	 {
		 count_ = fread(buf_, sizeof(target_type), sz_, f_);
		 ptr_ = count_ ? buf_ : nullptr;
	 }
	 return (count_ > 0) ? 1 : 0;
}


/** thread function **/
void exec(chunk&& c, const char* infile, std::queue<std::exception_ptr> qe)
{  
	try
	{
		FILE* f = nullptr;
		f = fopen(infile, "rb");
		if (f == nullptr)
		{
			std::cout << "File not found: " << infile << "\n";
			return;
		}
		fseeko64(f, c.offset_, SEEK_SET);

		size_t data_size = c.sz_ / sizeof(target_type);
		size_t buf_size = c.mem_ / sizeof(target_type);
		size_t thr_rpt = (data_size + buf_size - 1) / buf_size;					//thread repeat cnt

		c.buf_ = new target_type[buf_size];

		size_t data_counter = 0;
		for (unsigned int i = 0; i < thr_rpt; ++i)
		{
			size_t uniq = c.id_ * thr_rpt + i;
			std::stringstream ss;
			ss << "tmp_" << uniq;
			std::string tmpfile = ss.str();

			size_t read_cnt = (data_counter + buf_size) <= data_size
				? buf_size
				: data_size - data_counter;

			size_t cnt = fread(c.buf_, sizeof(target_type), read_cnt, f);
			data_counter += cnt;

			if (cnt < 1)
				break;

			std::sort((target_type*)(&c.buf_[0]), (target_type*)(&c.buf_[cnt])); // , std::less<target_type>());

			FILE* ftmp = nullptr;
			ftmp = fopen(tmpfile.c_str(), "wb");
			if (ftmp == nullptr)
			{
				std::lock_guard<std::mutex> lock(omtx);
				std::cout << "Cannot open tmp file: " << tmpfile << "\n";
				c.state_ = 1;
				return;
			}
			size_t wcnt = fwrite(c.buf_, sizeof(target_type), cnt, ftmp);
			if (wcnt != cnt)
			{
				std::lock_guard<std::mutex> lock(omtx);
				std::cout << "Error write tmp file: " << tmpfile << " to write=" << cnt << " actual=" << wcnt << "\n";
				c.state_ = 1;
				return;
			}

			fclose(ftmp);
			ftmp = nullptr;

			/** tmp file to list **/
			{
				std::lock_guard<std::mutex> lock(mtx);
				tmpFiles.push_back(new tmpf(tmpfile));
				std::cout << "Create sorted file: " << tmpfile << "\n";
			}
			if (data_counter >= data_size)	//read completed
				break;
		}
		fclose(f);
		f = nullptr;
	}
	catch (std::exception&) 
	{
		std::lock_guard<std::mutex> lock(omtx);
		qe.push(std::current_exception());
	}
}

/** target function
	params: mem_avail - whole memory available
			thr_count - number of threads to use
			infile - input file name
			outfile - output file name
	returns: result code = 0 : normal, sort comleted
						   1 : input file not found
						   2 : cannot create ootput file
						   3 : output file write error
						   4 : output file write error
						   5 : exception 
**/
int merge_sort(uint64_t mem_avail, unsigned int thr_count, const char* infile, const char* outfile)
{
/** analyse input file **/
	FILE* f = nullptr;
	f = fopen(infile, "rb");
	if (f == nullptr)
	{
		std::cout << "File not found: " << infile;
		return 1;
	}

	fseeko64(f, 0, SEEK_END);
	off64_t len = ftello64(f);
	fclose(f);
	f = nullptr;

	std::cout << "File: " << infile << " length=" << len << " bytes, " << len / sizeof(target_type) << " UINTs\n";

	int64_t fl = len;
	if (fl % sizeof(target_type) != 0)
	{
		fl = fl & ~(sizeof(target_type) - 1);										//alignment to uint size
		std::cout << "File doesn't fit " << target_name << " size (" << sizeof(target_type) << " bytes), truncate. Use file length: " 
			<< fl << " bytes, " << fl / sizeof(target_type) << " " << target_name << "s\n";
	}
	
/** calculate buffers, data size, etc. **/
	uint64_t mem_reserved = mem_avail / (thr_count + thr_count);				//for main thread
	uint64_t thrmem_sz = (mem_avail - mem_reserved) / thr_count;				//thread mem buffer
	thrmem_sz &= ~(sizeof(target_type) - 1);									//alignment to target_type size

	int64_t input_size = (fl + thr_count - 1) / thr_count + 1;					//thread data piece in file
	if (input_size & (sizeof(target_type) - 1))									//if not uint-aligned
	{
		input_size &= ~(sizeof(target_type) - 1);								//alignment to target_type size
		input_size += sizeof(target_type);
	}

//	std::cout << "Thread memory amount: " << thrmem_sz << " bytes. Data frame: " << input_size << " " << target_name << "s\n";

	time_t start, final;
	start = time(0);
	FILE* fw;					// output file
	target_type* outbuf;		// write buffer

/** execute partial sort 
create a set of temporary sorted files
**/
	try
	{
		std::vector<std::thread> thr;
		std::queue<std::exception_ptr> qe;
		for (unsigned int i = 0; i < thr_count; ++i)
		{
			int64_t offset = input_size * i;
			int64_t data_sz = (fl - offset) >= input_size 
				? input_size 
				: fl - offset;

			std::cout << "Create thread:" << i << " Offset=" << offset << " bytes, Data size=" << data_sz << " " << target_name << "s, Memory="  << thrmem_sz << " bytes\n";
			thr.push_back(std::thread(exec, chunk(i, offset, data_sz, thrmem_sz), infile, qe));
		}

		for (auto& thread : thr) { thread.join(); }			//partial sort completed
		thr.erase(thr.begin(), thr.end());
		if ( qe.empty() )
		{
			std::cout << "Partial sort completed, start merge.\n";
		}
		else
		{
			std::exception_ptr e;
			while ( !qe.empty() )
			{
				try
				{
					e = qe.front();
					qe.pop();
					std::rethrow_exception(e);
				}
				catch (std::exception& e)
				{
					std::cout << "Exception in sort thread: " << e.what();
				}
			}
			return 5;
		}

	/** merge result file from temporary files **/

	/** calc size for read/write buffers **/
		uint64_t tmp_buf_size = mem_avail / sizeof(target_type) / 2 / (tmpFiles.size() + 1);
		uint64_t bitmask = 1;
		while ((bitmask <<= 1) < tmp_buf_size); // { st <<= 1;	}
		tmp_buf_size = bitmask >> 1;
//		tmp_buf_size = (tmp_buf_size > 16384) ? 16384 : tmp_buf_size;
		std::cout << "Use tmp buf size: " << tmp_buf_size << " " << target_name << "s\n";

		uint64_t counter = 0;
		uint64_t wcounter = 0;
		outbuf = new target_type[tmp_buf_size];
		target_type* ptr = outbuf;
		fw = fopen(outfile, "wb");
		if (fw == nullptr)
		{
			std::cout << "Cannot open file: " << outfile << "\n";
			return 2;
		}

		size_t wcnt = 0;
		for (auto t : tmpFiles) 
		{ 
			t->init(tmp_buf_size); 
			t->next(); 
		}

		tmpFiles.sort([](const tmpf* a, const tmpf* b) { return a->value() < b->value(); });

		/** repeat until every tmp files ends **/
		std::string tn = "";
		while (tmpFiles.size() > 0)
		{
 			tmpf* tmp_file = *tmpFiles.begin();
			tn = tmp_file->filename_;
			*ptr++ = tmp_file->value();
			++counter;

			if (ptr == outbuf + tmp_buf_size)
			{
				wcnt = fwrite(static_cast<const void*>(outbuf), sizeof(target_type), size_t(ptr-outbuf), fw);
				wcounter += wcnt;
				if (wcnt != size_t(ptr - outbuf))
				{
					std::cout << "Error write outfile: " << outfile << " to write=" << size_t(ptr - outbuf) << " actual=" << wcnt << "\n";
					return 3;
				}
				ptr = outbuf;
			}
			if ( !tmp_file->next() )		// check EOF
			{
				fclose(tmp_file->f_);

				if (remove(tn.c_str()) != 0)
					std::cout << "Error deleting file:" << tn << "\n";
				else
					std::cout << "File processed: " << tn << "\n";

				tmpFiles.erase(tmpFiles.begin());
				continue;					// tmp file processed. Have to take value from next file (list first item).
			}

/**	after tmp_file->next()	find a place for next value in sorted list
	if found, then move first item to new position, so list become sorted again **/
			auto it = std::lower_bound(
				tmpFiles.begin(), 
				tmpFiles.end(), 
				tmp_file, 
				[](const tmpf* a, const tmpf* b) { return a->value() < b->value(); }
			);
			if ( it != tmpFiles.begin() )
			{
				tmpFiles.splice(it, tmpFiles, tmpFiles.begin());
			}
		}

		if (ptr > outbuf)
		{
			wcnt = fwrite(static_cast<const void*>(outbuf), sizeof(target_type), size_t(ptr - outbuf), fw);
			wcounter += wcnt;
			if (wcnt != size_t(ptr - outbuf))
			{
				std::cout << "Error write outfile: " << outfile << " to write=" << size_t(ptr - outbuf) << " actual=" << wcnt << " from:" << tn << "\n";
				return 4;
			}
		}
		std::cout << "Outfile: " << outfile << " size=" << wcounter * sizeof(target_type) << " uint=" << wcounter << "\n";
	}
	catch (std::exception& e)
	{
		std::cout << "Error: " << e.what() << "\n";
		return 5;
	}

	fflush(fw);
	fclose(fw);
	delete[] outbuf;
	for (auto t : tmpFiles) { if (t) delete t; }
	final = time(0);
	std::cout << final - start << " sec\n" << "\n";
	return 0;
}

/**  **/
int main()
{
	std::cout << "Multithread merge sort\n";

	constexpr uint64_t mem_avail = 128 * 1024 * 1024;

	unsigned int thr_count = std::thread::hardware_concurrency();
	std::cout << "Threads: " << thr_count << "\n";
	if (thr_count < 1) thr_count = 1;

	return merge_sort(mem_avail, thr_count, "input", "output");
}

