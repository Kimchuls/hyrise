#ifndef _RDMA_H_
#define _RDMA_H_
#include <cstdint>
#include <memory>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <cstdint>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <getopt.h>

#include <sys/time.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

/* poll CQ timeout in millisec (2 seconds) */
#define MAX_POLL_CQ_TIMEOUT 2000
#define MSG "SEND operation 36"
#define RDMAMSGR "RDMA read operation 36 "
#define RDMAMSGW "RDMA write operation 36"
// #define MSG_SIZE (strlen(MSG) + 1)
#define BUFF_SIZE 1024 * 1024 * 1024
// #define MSG_SIZE 6
#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif
namespace hyrise
{
    /* structure of test parameters */
    struct config_t
    {
        const char *dev_name; /* IB device name */
        char *server_name;    /* server host name */
        u_int32_t tcp_port;   /* server TCP port */
        int ib_port;          /* local IB port to work with */
        int gid_idx;          /* gid index to use */
    } __attribute__((packed));
    /* structure to exchange data which is needed to connect the QPs */
    struct cm_con_data_t
    {
        uint64_t addr;   /* Buffer address */
        uint32_t rkey;   /* Remote key */
        uint32_t qp_num; /* QP number */
        uint16_t lid;    /* LID of the IB port */
        uint8_t gid[16]; /* gid */
    } __attribute__((packed));

    /* structure of system resources */
    struct resources
    {
        struct ibv_device_attr device_attr; /* Device attributes */
        struct ibv_port_attr port_attr;     /* IB port attributes */
        struct cm_con_data_t remote_props;  /* values to connect to remote side */
        struct ibv_context *ib_ctx;         /* device handle */
        struct ibv_pd *pd;                  /* PD handle */
        struct ibv_cq *cq;                  /* CQ handle */
        struct ibv_qp *qp;                  /* QP handle */
        struct ibv_mr *mr;                  /* MR handle for buf */
        char *buf;                          /* memory buffer pointer, used for RDMA and send ops */
        int sock;                           /* TCP socket file descriptor */
    } __attribute__((packed));
    class RDMA_Manager
    {
    public:
        config_t config;
        resources *res;
        
        int sock_connect(const char *servername, int port);
        int sock_sync_data(int sock, int xfer_size, char *local_data, char *remote_data);
        int poll_completion();
        int post_send(int opcode);
        int post_receive();
        int resources_create();
        int modify_qp_to_init(struct ibv_qp *qp);
        int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid); // ready to receive
        int modify_qp_to_rts(struct ibv_qp *qp);                                                    // ready to send
        int connect_qp();
        int connect_qp_2();
        int resources_destroy();

        int init(config_t config);
        int RDMA_Send();
        int RDMA_Receive();
        int RDMA_Read();
        int RDMA_Write();
        int socket_connect();
        int device_connect();
        int sock_connect();
        int local_memory_register();
        int allocate_pd();
        int create_cq();
        int create_qp();

        void print_config(void);
        void usage(const char *argv0);
    protected:
        RDMA_Manager() = default;
        friend class Hyrise;

    private:
    };

}

#endif
