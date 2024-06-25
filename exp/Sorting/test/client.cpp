#include <iostream>
#include <cstring>
#include <vector>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define PORT 10086
#define BUFFER_SIZE 1024
const int kValLen = 2;

struct Request {
    uint64_t idx;
    uint32_t shard_id;
    bool shard_sort_finished;
    bool to_sort;
    bool end_of_req;
    bool waiting;
};

struct Val {
    char data[kValLen];
};

struct Response {
    uint32_t latest_shard_ip;
    bool found;
    Val val;
    bool shard_sort_finished;
    bool all_data_sorted;
    bool ending;
};

std::atomic<std::mutex> mutex_;
Request gen_req(nu::PerfThreadState *perf_state) {
    static std::atomic<uint64_t> shard_idx_ = 0;
    static std::atomic<uint64_t> reload_idx = 0;
    Request req;
    auto perf_req = std::make_unique<PerfReq>();
    random_int(&req.idx);
    req.end_of_req = false;
    req.waiting = false;
    req.shard_id = -1;
       
    mutex_.lock();
    if(req_end) {
        std::cout << "end_of_req" << std::endl;
        perf_req->req.end_of_req = true;
        mutex_.unlock();
    } 
    else if(sorting && !sorted) {
        perf_req->req.waiting = true;
        // perf_req->req.to_sort = true;
        mutex_.unlock();
    } 
    else {
        if(shard_idx_ >= (1 << DSVector::kDefaultPowerNumShards)) {
            perf_req->req.shard_sort_finished = true;
            std::cout << "send to sort" << std::endl;
            sorting = true;
            perf_req->req.to_sort = true;
            mutex_.unlock();
        } else {
            perf_req->req.shard_id = shard_idx_++;
            // if(shard_idx_ == (1 << DSVector::kDefaultPowerNumShards)) {
            //     shardsorting = true;
            //     std::cout << "all shard sorting request sent" << std::endl;
            // }
            perf_req->req.shard_sort_finished = false;
            mutex_.unlock();
        }
    }
    // else {
    //     // std::cout << "send waiting" << std::endl;
    //     perf_req->req.waiting = true;
    //     mutex_.unlock();
    // }
    
    
    return perf_req;
}

void sendRequest(int sock, const Request& request) {
    send(sock, &request, sizeof(request), 0);
    std::cout << "Request sent to server: "
              << "idx=" << request.idx << ", "
              << "shard_id=" << request.shard_id << ", "
              << "shard_sort_finished=" << request.shard_sort_finished << ", "
              << "to_sort=" << request.to_sort << ", "
              << "end_of_req=" << request.end_of_req << ", "
              << "waiting=" << request.waiting << std::endl;
}

Response receiveResponse(int sock) {
    Response response;
    int valread = read(sock, &response, sizeof(response));
    if (valread > 0) {
        std::cout << "Response received from server: "
                  << "latest_shard_ip=" << response.latest_shard_ip << ", "
                  << "found=" << response.found << ", "
                  << "val.data=" << response.val.data << ", "
                  << "shard_sort_finished=" << response.shard_sort_finished << ", "
                  << "all_data_sorted=" << response.all_data_sorted << ", "
                  << "ending=" << response.ending << std::endl;
    }
    return response;
}

int main() {
    int sock = 0;
    struct sockaddr_in serv_addr;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cerr << "Socket creation error" << std::endl;
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        std::cerr << "Invalid address / Address not supported" << std::endl;
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Connection Failed" << std::endl;
        return -1;
    }

    std::vector<Request> requests = {
        {1, 1, true, false, false, true},
        {2, 2, false, true, false, false},
        {3, 3, true, true, true, false}
    };

    for (const auto& request : requests) {
        sendRequest(sock, request);
        Response response = receiveResponse(sock);
        std::cout << "Received response for request idx: " << request.idx << std::endl;
    }

    close(sock);
    return 0;
}
