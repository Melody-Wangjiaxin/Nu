#include <iostream>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
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

void handleClient(int client_socket) {
    Request request;
    int valread;

    while ((valread = read(client_socket, &request, sizeof(request))) > 0) {
        std::cout << "Received from client: "
                  << "idx=" << request.idx << ", "
                  << "shard_id=" << request.shard_id << ", "
                  << "shard_sort_finished=" << request.shard_sort_finished << ", "
                  << "to_sort=" << request.to_sort << ", "
                  << "end_of_req=" << request.end_of_req << ", "
                  << "waiting=" << request.waiting << std::endl;

        Response response = {12345, true, {}, request.shard_sort_finished, true, false};
        std::strcpy(response.val.data, "Server response data");

        send(client_socket, &response, sizeof(response), 0);
        std::cout << "Response sent to client: "
                  << "latest_shard_ip=" << response.latest_shard_ip << ", "
                  << "found=" << response.found << ", "
                  << "val.data=" << response.val.data << ", "
                  << "shard_sort_finished=" << response.shard_sort_finished << ", "
                  << "all_data_sorted=" << response.all_data_sorted << ", "
                  << "ending=" << response.ending << std::endl;
    }

    if (valread == 0) {
        std::cout << "Client disconnected" << std::endl;
    } else if (valread < 0) {
        perror("Read error");
    }

    close(client_socket);
}

int main() {
    int server_fd, client_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("Socket failed");
        exit(EXIT_FAILURE);
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 3) < 0) {
        perror("Listen");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    std::cout << "Server is listening on port " << PORT << std::endl;

    while (true) {
        if ((client_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("Accept");
            close(server_fd);
            exit(EXIT_FAILURE);
        }

        std::cout << "New connection established" << std::endl;
        handleClient(client_socket);
    }

    close(server_fd);
    return 0;
}
