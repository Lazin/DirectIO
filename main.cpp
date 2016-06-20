#include <iostream>
#include <cstdio>
#include <stdexcept>
#include <unistd.h>
#include <thread>
#include <vector>
#include <cstring>
#include <time.h>

#include <sys/fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <uv.h>

void on_open(uv_fs_t *req);
void on_read(uv_fs_t *req);
void on_write(uv_fs_t *req);

const int N_STREAMS=1;

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
    const size_t FSIZE = 4*1024*1024*1024l;
    //const size_t FSIZE = 400*1024*1024l;
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
            }
            return;
        }
        // Perform next write
        uv_fs_write(uv_default_loop(),
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
            uv_fs_write(uv_default_loop(),
                        &writes[i],
                        static_cast<uv_file>(open_req.result),
                        &ureq->buffer,
                        1,
                        static_cast<int64_t>(off),
                        on_write);
        }
    }
}

int main() {
    std::cout << "Creating " << PATH << " file" << std::endl;
    try {
        init_file();
        for (size_t i = 0; i < N_STREAMS; i++) {
            Req* req = new Req();
            req->path = PATH,
            req->current_offset = i*BUFFER_SIZE,
            req->stride = N_STREAMS*BUFFER_SIZE,
            req->cnt = 0;
            create_buf(&req->buffer);
            writes[i].data = req;
        }
        PerfTimer total;
        uv_fs_open(uv_default_loop(), &open_req, PATH, O_DIRECT|O_RDWR, 0, &on_open);
        uv_run(uv_default_loop(), UV_RUN_DEFAULT);
        uv_loop_close(uv_default_loop());
        std::cout << "Done writing " << ((double)FSIZE/1024.0/1024.0) << "MB in "
                  << total.elapsed() << "s" << std::endl;
    } catch (const std::exception& e) {
        std::cout << "error main " << e.what() << std::endl;
    }
    return 0;
}
