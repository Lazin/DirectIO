#include <iostream>
#include <cstdio>
#include <stdexcept>
#include <unistd.h>
#include <thread>
#include <vector>
#include <cstring>
#include <time.h>

#include <atomic>
#include <thread>
#include <mutex>
#include <queue>
#include <functional>
#include <future>

#include <sys/fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <uv.h>

typedef uint8_t  u8;
typedef int8_t   i8;
typedef uint16_t u16;
typedef int16_t  i16;
typedef uint32_t u32;
typedef int32_t  i32;
typedef uint64_t u64;
typedef int64_t  i64;
typedef u64 LogicAddr;
enum aku_Status {
    AKU_SUCCESS,
    AKU_ENO_DATA,
    AKU_EBAD_ARG,
};

namespace IO {

// Memory block

//! Represents memory block
class Block {
    std::vector<u8>           data_;
    LogicAddr                 addr_;

public:
    Block(LogicAddr addr, std::vector<u8>&& data);

    const u8* get_data() const;

    size_t get_size() const;

    LogicAddr get_addr() const;
};
Block::Block(LogicAddr addr, std::vector<u8>&& data)
    : data_(std::move(data))
    , addr_(addr)
{
}

const u8* Block::get_data() const {
    return data_.data();
}

size_t Block::get_size() const {
    return data_.size();
}

LogicAddr Block::get_addr() const {
    return addr_;
}

// Future

template<class ...Args>
struct TaskWrapper {
    // Storage for packaged_task
    struct _ICallable {
        virtual void call(Args... args) = 0;
    };

    template<class Fn>
    struct Callable : _ICallable {
        Fn fn_;

        Callable(Fn&& f)
            : fn_(std::move(f))
        {}

        virtual void call(Args... args) {
            fn_(args...);
        }
    };

    std::unique_ptr<_ICallable> fn_;

    TaskWrapper() {}

    template<class Ret>
    TaskWrapper(std::packaged_task<Ret(Args...)>&& fn)
    {
        auto ptr = new Callable<std::packaged_task<Ret(Args...)>>(std::move(fn));
        fn_.reset(ptr);
    }

    template<class Ret>
    TaskWrapper& operator = (std::packaged_task<Ret(Args...)>&& fn) {
        this->~TaskWrapper();
        new (this) TaskWrapper(std::move(fn));
        return *this;
    }

    void operator () (Args... args) const {
        fn_->call(args...);
    }
};

template<class ...T>
struct FutureState {
    bool initialized_;
    std::tuple<T...> tuple_;
    TaskWrapper<T...> continuation_;

    FutureState()
        : initialized_(false)
    {
    }

    void set(T... values) {
        if (!initialized_) {
            tuple_ = std::make_tuple(values...);
            initialized_ = true;
        } else {
            std::cout << "term1" << std::endl;
            std::terminate();
        }
    }

    std::tuple<T...> get_values() const {
        return tuple_;
    }

    template<class Fn>
    void register_continuation(Fn const& f) {
        continuation_ = f;
    }

    void call() {
        if (initialized_ && continuation_) {
            call_func(continuation_, std::index_sequence_for<T...>{});
        } else {
            std::cout << "term2" << std::endl;
            std::terminate();
        }
    }

    bool conditional_call() {
        if (initialized_) {
            call_func(continuation_, std::index_sequence_for<T...>{});
            return true;
        }
        return false;
    }

private:
    template<class Func, std::size_t ...I>
    void call_func(Func const& func, std::index_sequence<I...>)
    {
        func(std::get<I>(tuple_)...);
    }
};

template <class ...T>
struct Future {
    FutureState<T...> &state_;

    Future(FutureState<T...>& state) : state_(state)
    {
    }

    template<class Fn>
    Future<T...>& then(Fn const& f) {
        state_.register_continuation(f);
        return *this;
    }

};

template <class ...Arg>
struct Promise {
    FutureState<Arg...> state_;

    void set_value(Arg... values) {
        state_.set(values...);
        make_ready();
    }

    Future<Arg...> get_future() {
        return Future<Arg...>(state_);
    }

    void make_ready() {
        state_.conditional_call();
    }
};

// IO-service

class IOService {
    uv_loop_t loop_;
    uv_idle_t idle_;
    std::atomic_bool stop_;
    // task queue
    mutable std::mutex mutex_;
    //std::queue<std::packaged_task<void()>> tasks_;

    static void on_idle(uv_idle_t* idle) {
        IOService* io = static_cast<IOService*>(idle->data);
        if (io->stop_) {
            uv_stop(&io->loop_);
        }
    }

public:
    IOService()
        : stop_{0}
    {
        uv_loop_init(&loop_);
        uv_idle_init(&loop_, &idle_);
        uv_idle_start(&idle_, &IOService::on_idle);
        idle_.data = this;
        loop_.data = this;
    }

    void stop() {
        stop_ = true;
    }

    uv_loop_t* _get_loop() {
        return &loop_;
    }

    void run() {
        uv_run(&loop_, UV_RUN_DEFAULT);
    }

    void close() {
        uv_loop_close(&loop_);
    }

    template<class Fn>
    Future<> post(Fn const& fn) {
        throw "not implemented";
    }
};

class File : std::enable_shared_from_this<File> {
    IOService& io_;
    uv_fs_t req_;
    uv_file fd_;

    Promise<aku_Status> open_promise_;

    static void on_open(uv_fs_t *req) {
        uv_fs_req_cleanup(req);
        File* file = static_cast<File*>(req->data);
        if (req->result < 0) {
            file->open_promise_.set_value(AKU_EBAD_ARG);  // real error
        } else {
            file->fd_ = req->result;
            file->open_promise_.set_value(AKU_SUCCESS);
        }
    }

public:
    File(IOService& io)
        : io_(io)
        , fd_(0)
    {
        req_.data = this;
    }

    Future<aku_Status> open(const char* path, int flags) {
        uv_fs_open(io_._get_loop(), &req_, path, flags, 0, &File::on_open);
        return open_promise_.get_future();
    }
};



struct BlockStore {

    virtual ~BlockStore() = default;

    /** Read block from blockstore
      */
    //virtual Future<aku_Status, std::shared_ptr<Block>> read_block(LogicAddr addr) = 0;

    /** Add block to blockstore.
      * @param data Pointer to buffer.
      * @return Status and block's logic address.
      */
    virtual std::tuple<aku_Status, LogicAddr> append_block(u8 const* data) = 0;

    //! Flush all pending changes.
    virtual void flush() = 0;

    //! Check if addr exists in block-store
    virtual bool exists(LogicAddr addr) const = 0;

    //! Compute checksum of the input data.
    virtual u32 checksum(u8 const* begin, size_t size) const = 0;
};

/* Usage:
 * auto fs = LocalFileSystem::get_blockstore("path/to/storage");
 * fs.read_block_async(addr).then([fs] (aku_Status status, std::shared_ptr<Block> block) {
 *      ...
 *      std::unique_ptr<Block> wblock;
 *      ...
 *      fs.append_block_async(std::move(wblock)).then([] (aku_Status status, LogicAddr addr) {
 *          ....
 *          upper_level_node.append(payload);
 *      });;
 * });
 */
}

void on_open(uv_fs_t *req);
void on_read(uv_fs_t *req);
void on_write(uv_fs_t *req);

const int N_STREAMS=2;
static volatile bool done = false;

static uv_fs_t open_req;
//static uv_fs_t read_req;
static uv_fs_t writes[N_STREAMS];
//static uv_fs_t close_req;

//static uv_buf_t buffer;

class PerfTimer
{
public:
    PerfTimer() {
        clock_gettime(CLOCK_MONOTONIC_RAW, &_start_time);
    }

    void restart() {
        clock_gettime(CLOCK_MONOTONIC_RAW, &_start_time);
    }

    double elapsed() const {
        timespec curr;
        clock_gettime(CLOCK_MONOTONIC_RAW, &curr);
        return double(curr.tv_sec - _start_time.tv_sec) +
               double(curr.tv_nsec - _start_time.tv_nsec)/1000000000.0;
    }
private:
    timespec _start_time;
};


namespace {
    const char* PATH = "/tmp/perftest.tmp";
    //const size_t FSIZE = 4*1024*1024*1024l;
    const size_t FSIZE = 40*1024*1024l;
    const size_t BUFFER_SIZE = 4096;

    int create_file() {
        auto fd = open(PATH, O_DIRECT|O_RDWR|O_SYNC);
        if (fd == -1) {
            throw std::runtime_error(std::string("open O_DIRECT error: ") + strerror(errno));
        }
        return fd;
    }

    void truncate(int fd) {
        if (ftruncate(fd, FSIZE) == -1) {
            throw std::runtime_error(std::string("truncate error: ") + strerror(errno));
        }
    }

    void close_file(int fd) {
        if (close(fd) != 0) {
            throw std::runtime_error(std::string("close error: ") + strerror(errno));
        }
    }

    void init_file() {
        system("rm /tmp/perftest.tmp");
        int fd = open(PATH, O_CREAT|O_WRONLY|O_EXCL, S_IRUSR|S_IWUSR);
        if (fd == -1) {
            if (errno != EEXIST) {
            throw std::runtime_error(std::string("create error: ") + strerror(errno));
            }
        } else {
            truncate(fd);
            close_file(fd);
        }
    }

    void create_buf(uv_buf_t* iov) {
        char* p = static_cast<char*>(aligned_alloc(BUFFER_SIZE, BUFFER_SIZE));
        iov[0] = uv_buf_init(p, BUFFER_SIZE);
        for (size_t i = 0; i < BUFFER_SIZE; i++) {
            p[i] = 32 + rand()%10;
        }
    }
}

struct Req {
    const char*     path;
    size_t          current_offset;
    size_t          stride;
    size_t          cnt;
    uv_buf_t        buffer;
};


void on_write(uv_fs_t* req) {
    uv_fs_req_cleanup(req);
    Req* ureq = static_cast<Req*>(req->data);
    static int streams = N_STREAMS;
    if (req->result < 0) {
        std::cout << "Write error: " << uv_strerror(static_cast<int>(req->result)) << std::endl;
    } else {
        auto off = ureq->current_offset;
        ureq->current_offset += ureq->stride;
        ureq->cnt++;
        if (off >= FSIZE) {
            // Done writing
            std::cout << "Done!" << std::endl;
            std::cout << "Final offset = " << off << ", cnt: " << ureq->cnt << std::endl;
            streams--;
            if (streams == 0) {
                uv_fs_close(uv_default_loop(), req, static_cast<uv_file>(open_req.result), nullptr);
                done = true;
            }
            return;
        }
        // Perform next write
        uv_fs_write(req->loop,
                    req,
                    static_cast<uv_file>(open_req.result),
                    &ureq->buffer,
                    1,
                    static_cast<int64_t>(off),
                    on_write);
    }
}

void on_open(uv_fs_t* req) {
    uv_fs_req_cleanup(req);
    if (req->result < 0) {
        std::cout << "Open error: " << uv_strerror(static_cast<int>(req->result)) << std::endl;
    } else {
        std::cout << "File opened correctly" << std::endl;
        for (int i = 0; i < N_STREAMS; i++) {
            auto ureq = static_cast<Req*>(writes[i].data);
            auto off = ureq->current_offset;
            ureq->current_offset += ureq->stride;
            uv_fs_write(req->loop,
                        &writes[i],
                        static_cast<uv_file>(open_req.result),
                        &ureq->buffer,
                        1,
                        static_cast<int64_t>(off),
                        on_write);
        }
    }
}

void check_file_content() {
    uv_fs_t open;
    uv_fs_t rdreq;
    auto status = uv_fs_open(uv_default_loop(), &open, PATH, O_RDONLY, 0, nullptr);
    if (status < 0) {
        std::cout << "Error: " << status << std::endl;
        return;
    }
    std::vector<char> data(BUFFER_SIZE, 0);
    uv_buf_t buf = uv_buf_init(data.data(), BUFFER_SIZE);
    for(ssize_t off = 0; off < static_cast<ssize_t>(FSIZE); off += static_cast<ssize_t>(BUFFER_SIZE)) {
        status = uv_fs_read(uv_default_loop(), &rdreq, static_cast<uv_file>(open.result), &buf, 1, off, nullptr);
        if (status < 0) {
            std::cout << "Failure! Read error at " << off << std::endl;
            return;
        }
        ssize_t stride = (off / BUFFER_SIZE) % N_STREAMS;
        auto ureq = static_cast<Req*>(writes[stride].data);
        for (size_t i = 0; i < BUFFER_SIZE; i++) {
            if (ureq->buffer.base[i] != data[i]) {
                std::cout << "Failure! Mismatch at " << off << std::endl;
                return;
            }
        }
    }
    std::cout << "Success" << std::endl;
}


void idle_cb(uv_idle_t* idle) {
    if (done) {
        std::cout << "stopped" << std::endl;
        uv_stop(idle->loop);
    }
}

int main() {
    using namespace IO;
    std::cout << "Creating " << PATH << " file" << std::endl;
    try {
        init_file();
        /*
        for (size_t i = 0; i < N_STREAMS; i++) {
            Req* req = new Req();
            req->path = PATH,
            req->current_offset = i*BUFFER_SIZE,
            req->stride = N_STREAMS*BUFFER_SIZE,
            req->cnt = 0;
            create_buf(&req->buffer);
            writes[i].data = req;
        }
        uv_loop_t loop;
        if (uv_loop_init(&loop) < 0) {
            std::cout << "Can't create loop!" << std::endl;
        }
        std::thread th([&]() {
            uv_idle_t idle;
            uv_idle_init(&loop, &idle);
            uv_idle_start(&idle, idle_cb);
            uv_run(&loop, UV_RUN_DEFAULT);
            std::cout << "!uv_run exited!" << std::endl;
            uv_loop_close(&loop);
        });
        PerfTimer total;
        int flags = O_DIRECT|O_RDWR;
        uv_fs_open(&loop, &open_req, PATH, flags, 0, &on_open);
        th.join();
        std::cout << "Done writing " << ((double)FSIZE/1024.0/1024.0) << "MB in "
                  << total.elapsed() << "s" << std::endl;
        check_file_content();
        IOService io;
        std::thread th([&]() {
            io.run();
        });
        File file(io);
        auto future = file.open(PATH, O_RDONLY);
        th.join();
        */
        /*
        FutureState<int, std::string, double> tup;
        tup.set(42, "hello", 3.14159);
        auto fn = [](int a, std::string b, double c) {
            std::cout << "a=" << a << ", b=" << b << ", c=" << c << std::endl;
        };
        tup.register_continuation(fn);
        tup.call();
        */
        std::packaged_task<int(double)> task([](double val) {
            std::cout << "val = " << val << std::endl;
            return 42;
        });
        std::future<int> f = task.get_future();
        TaskWrapper<double> func(std::move(task));
        std::cout << "before call " << f.valid() << std::endl;
        func(3.14159);
        std::cout << "after call " << f.valid() << std::endl;
        std::cout << "result     " << f.get() << std::endl;
    } catch (const std::exception& e) {
        std::cout << "error main " << e.what() << std::endl;
    }
    return 0;
}
