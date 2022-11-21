#include "rdma.h"
#include "hyrise.hpp"
namespace hyrise
{

    int RDMA_Manager::sock_connect(const char *servername, int port)
    {
        struct addrinfo *resolved_addr = NULL;
        struct addrinfo *iterator;
        char service[6];
        int sockfd = -1;
        int listenfd = 0;
        int tmp;
        struct addrinfo hints =
            {
                .ai_flags = AI_PASSIVE,
                .ai_family = AF_INET,
                .ai_socktype = SOCK_STREAM};
        if (sprintf(service, "%d", port) < 0)
            goto sock_connect_exit;
        /* Resolve DNS address, use sockfd as temp storage */
        sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
        if (sockfd < 0)
        {
            fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
            goto sock_connect_exit;
        }
        /* Search through results and find the one we want */
        for (iterator = resolved_addr; iterator; iterator = iterator->ai_next)
        {
            sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
            // 设置套接字选项避免地址使用错误
            int on = 1;
            if ((setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) < 0)
            {
                perror("setsockopt failed");
                exit(EXIT_FAILURE);
            }
            if (sockfd >= 0)
            {
                /* Client mode. Initiate connection to remote */
                // if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen)))
                // {
                //     fprintf(stdout, "failed connect \n");
                //     close(sockfd);
                //     sockfd = -1;
                // }
                for (int x = 0; x < 10; x++)
                {
                    if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen)))
                    {
                        fprintf(stdout, "failed connect, wait in %d time(s) \n", x + 1);
                        if (x == 9)
                        {
                            close(sockfd);
                            sockfd = -1;
                        }
                        sleep(1);
                    }
                    else
                    {
                        break;
                    }
                }
            }
        }
    sock_connect_exit:
        if (listenfd)
            close(listenfd);
        if (resolved_addr)
            freeaddrinfo(resolved_addr);
        if (sockfd < 0)
        {
            if (servername)
                fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
            else
            {
                perror("server accept");
                fprintf(stderr, "accept() failed\n");
            }
        }
        return sockfd;
    }

    int RDMA_Manager::socket_connect()
    {
        res->sock = sock_connect(config.server_name, config.tcp_port);
        if (res->sock < 0)
        {
            fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n",
                    config.server_name, config.tcp_port);
            return 1;
        }
        fprintf(stdout, "TCP connection was established\n");
        return 0;
    }

    int RDMA_Manager::device_connect()
    {
        int i;
        struct ibv_device **dev_list = NULL;
        struct ibv_device *ib_dev = NULL;
        int num_devices;
        int rc = 0;
        fprintf(stdout, "searching for IB devices in host\n");
        /* get device names in the system */
        dev_list = ibv_get_device_list(&num_devices);
        if (!dev_list)
        {
            fprintf(stderr, "failed to get IB devices list\n");
            rc = 1;
            goto device_connect_exit;
        }
        /* if there isn't any IB device in host */
        if (!num_devices)
        {
            fprintf(stderr, "found %d device(s)\n", num_devices);
            rc = 1;
            goto device_connect_exit;
        }
        fprintf(stdout, "found %d device(s)\n", num_devices);
        /* search for the specific device we want to work with */
        for (i = 0; i < num_devices; i++)
        {
            if (!config.dev_name)
            {
                config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
                fprintf(stdout, "device not specified, using first one found: %s\n", config.dev_name);
            }
            if (!strcmp(ibv_get_device_name(dev_list[i]), config.dev_name))
            {
                ib_dev = dev_list[i];
                break;
            }
        }
        /* if the device wasn't found in host */
        if (!ib_dev)
        {
            fprintf(stderr, "IB device %s wasn't found\n", config.dev_name);
            rc = 1;
            goto device_connect_exit;
        }
        /* get device handle */
        res->ib_ctx = ibv_open_device(ib_dev);
        if (!res->ib_ctx)
        {
            fprintf(stderr, "failed to open device %s\n", config.dev_name);
            rc = 1;
            goto device_connect_exit;
        }
        /* query port properties */
        if (ibv_query_port(res->ib_ctx, config.ib_port, &res->port_attr))
        {
            fprintf(stderr, "ibv_query_port on port %u failed\n", config.ib_port);
            rc = 1;
            goto device_connect_exit;
        }
    device_connect_exit:
        ibv_free_device_list(dev_list); /* We are now done with device list, free it */
        dev_list = NULL;
        ib_dev = NULL;
        return rc;
    }

    int RDMA_Manager::allocate_pd()
    {
        /* allocate Protection Domain */
        res->pd = ibv_alloc_pd(res->ib_ctx);
        if (!res->pd)
        {
            fprintf(stderr, "ibv_alloc_pd failed\n");
            return 1;
        }
        return 0;
    }

    int RDMA_Manager::local_memory_register()
    {
        size_t size;
        /* allocate the memory buffer that will hold the data */
        size = BUFF_SIZE;
        res->buf = (char *)malloc(size);
        if (!res->buf)
        {
            fprintf(stderr, "failed to malloc %ld bytes to memory buffer\n", size);
            return 1;
        }
        memset(res->buf, 0, size);
        /* only in the server side put the message in the memory buffer */
        /* register the memory buffer */
        int mr_flags = 0;
        mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        res->mr = ibv_reg_mr(res->pd, res->buf, size, mr_flags);
        if (!res->mr)
        {
            fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
            return 1;
        }
        fprintf(stdout, "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n", res->buf, res->mr->lkey, res->mr->rkey, mr_flags);
        return 0;
    }

    int RDMA_Manager::create_cq()
    {
        int cq_size = 20;
        /* each side will send only one WR, so Completion Queue with 1 entry is enough */
        res->cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
        if (!res->cq)
        {
            fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
            return 1;
        }
        return 0;
    }

    int RDMA_Manager::create_qp()
    {
        struct ibv_qp_init_attr qp_init_attr;
        /* create the Queue Pair */
        memset(&qp_init_attr, 0, sizeof(qp_init_attr));
        qp_init_attr.qp_type = IBV_QPT_RC;
        qp_init_attr.sq_sig_all = 0;
        qp_init_attr.send_cq = res->cq;
        qp_init_attr.recv_cq = res->cq;
        qp_init_attr.cap.max_send_wr = 2500;
        qp_init_attr.cap.max_recv_wr = 2500;
        qp_init_attr.cap.max_send_sge = 30;
        qp_init_attr.cap.max_recv_sge = 30;
        res->qp = ibv_create_qp(res->pd, &qp_init_attr);
        if (!res->qp)
        {
            fprintf(stderr, "failed to create QP\n");
            return 1;
        }
        fprintf(stdout, "QP was created, QP number=0x%x\n", res->qp->qp_num);
        return 0;
    }

    int RDMA_Manager::resources_create()
    {
        int rc = 0;
        if (socket_connect())
        {
            rc = 1;
            goto resources_create_exit;
        }
        if (device_connect())
        {
            rc = 1;
            goto resources_create_exit;
        }
        if (allocate_pd())
        {
            rc = 1;
            goto resources_create_exit;
        }
        if (local_memory_register())
        {
            rc = 1;
            goto resources_create_exit;
        }
        if (create_cq())
        {
            rc = 1;
            goto resources_create_exit;
        }
        if (create_qp())
        {
            rc = 1;
            goto resources_create_exit;
        }
    resources_create_exit:
        if (rc)
        {
            /* Error encountered, cleanup */
            if (res->qp)
            {
                ibv_destroy_qp(res->qp);
                res->qp = NULL;
            }
            if (res->mr)
            {
                ibv_dereg_mr(res->mr);
                res->mr = NULL;
            }
            if (res->buf)
            {
                free(res->buf);
                res->buf = NULL;
            }
            if (res->cq)
            {
                ibv_destroy_cq(res->cq);
                res->cq = NULL;
            }
            if (res->pd)
            {
                ibv_dealloc_pd(res->pd);
                res->pd = NULL;
            }
            if (res->ib_ctx)
            {
                ibv_close_device(res->ib_ctx);
                res->ib_ctx = NULL;
            }
            if (res->sock >= 0)
            {
                if (close(res->sock))
                    fprintf(stderr, "failed to close socket\n");
                res->sock = -1;
            }
        }
        return rc;
    }

    int RDMA_Manager::sock_sync_data(int sock, int xfer_size, char *local_data, char *remote_data)
    {
        int rc;
        int read_bytes = 0;
        int total_read_bytes = 0;
        rc = write(sock, local_data, xfer_size);
        if (rc < xfer_size)
            fprintf(stderr, "Failed writing data during sock_sync_data\n");
        else
            rc = 0;
        while (!rc && total_read_bytes < xfer_size)
        {
            read_bytes = read(sock, remote_data, xfer_size);
            if (read_bytes > 0)
                total_read_bytes += read_bytes;
            else
                rc = read_bytes;
        }
        return rc;
    }

    int RDMA_Manager::poll_completion()
    {
        struct ibv_wc wc;
        unsigned long start_time_msec;
        unsigned long cur_time_msec;
        struct timeval cur_time;
        int poll_result;
        int rc = 0;
        /* poll the completion for a while before giving up of doing it .. */
        gettimeofday(&cur_time, NULL);
        start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
        do
        {
            poll_result = ibv_poll_cq(res->cq, 1, &wc);
            gettimeofday(&cur_time, NULL);
            cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
        } while ((poll_result == 0) && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
        if (poll_result < 0)
        {
            /* poll CQ failed */
            fprintf(stderr, "poll CQ failed\n");
            rc = 1;
        }
        else if (poll_result == 0)
        { /* the CQ is empty */
            fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
            rc = 1;
        }
        else
        {
            /* CQE found */
            // fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);
            /* check the completion status (here we don't care about the completion opcode */
            if (wc.status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", wc.status,
                        wc.vendor_err);
                rc = 1;
            }
        }
        return rc;
    }

    int RDMA_Manager::post_send(int opcode)
    {
        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad_wr = NULL;
        int rc;
        /* prepare the scatter/gather entry */
        memset(&sge, 0, sizeof(sge));
        sge.addr = (uintptr_t)res->buf;
        sge.length = BUFF_SIZE;
        sge.lkey = res->mr->lkey;
        /* prepare the send work request */
        memset(&sr, 0, sizeof(sr));
        sr.next = NULL;
        sr.wr_id = 0;
        sr.sg_list = &sge;
        sr.num_sge = 1;
        sr.opcode = static_cast<ibv_wr_opcode>(opcode);
        sr.send_flags = IBV_SEND_SIGNALED;
        if (opcode != IBV_WR_SEND)
        {
            sr.wr.rdma.remote_addr = res->remote_props.addr;
            sr.wr.rdma.rkey = res->remote_props.rkey;
        }
        /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
        rc = ibv_post_send(res->qp, &sr, &bad_wr);
        if (rc)
            fprintf(stderr, "failed to post SR\n");
        else
        {
            switch (opcode)
            {
            case IBV_WR_SEND:
                fprintf(stdout, "Send Request was posted\n");
                break;
            case IBV_WR_RDMA_READ:
                fprintf(stdout, "RDMA Read Request was posted\n");
                break;
            case IBV_WR_RDMA_WRITE:
                fprintf(stdout, "RDMA Write Request was posted\n");
                break;
            default:
                fprintf(stdout, "Unknown Request was posted\n");
                break;
            }
        }
        return rc;
    }

    int RDMA_Manager::post_receive()
    {
        struct ibv_recv_wr rr;
        struct ibv_sge sge;
        struct ibv_recv_wr *bad_wr;
        int rc;
        /* prepare the scatter/gather entry */
        memset(&sge, 0, sizeof(sge));
        sge.addr = (uintptr_t)res->buf;
        sge.length = BUFF_SIZE;
        sge.lkey = res->mr->lkey;
        /* prepare the receive work request */
        memset(&rr, 0, sizeof(rr));
        rr.next = NULL;
        rr.wr_id = 0;
        rr.sg_list = &sge;
        rr.num_sge = 1;
        /* post the Receive Request to the RQ */
        rc = ibv_post_recv(res->qp, &rr, &bad_wr);
        if (rc)
            fprintf(stderr, "failed to post RR\n");
        else
            fprintf(stdout, "Receive Request was posted\n");
        return rc;
    }

    int RDMA_Manager::modify_qp_to_init(struct ibv_qp *qp)
    {
        struct ibv_qp_attr attr;
        int flags;
        int rc;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.port_num = config.ib_port;
        attr.pkey_index = 0;
        attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
        rc = ibv_modify_qp(qp, &attr, flags);
        if (rc)
            fprintf(stderr, "failed to modify QP state to INIT\n");
        return rc;
    }

    int RDMA_Manager::modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid)
    {
        struct ibv_qp_attr attr;
        int flags;
        int rc;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTR;
        attr.path_mtu = IBV_MTU_256;
        attr.dest_qp_num = remote_qpn;
        attr.rq_psn = 0;
        attr.max_dest_rd_atomic = 1;
        attr.min_rnr_timer = 0x12;
        attr.ah_attr.is_global = 0;
        attr.ah_attr.dlid = dlid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num = config.ib_port;
        if (config.gid_idx >= 0)
        {
            attr.ah_attr.is_global = 1;
            attr.ah_attr.port_num = 1;
            memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
            attr.ah_attr.grh.flow_label = 0;
            attr.ah_attr.grh.hop_limit = 1;
            attr.ah_attr.grh.sgid_index = config.gid_idx;
            attr.ah_attr.grh.traffic_class = 0;
        }
        flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
        rc = ibv_modify_qp(qp, &attr, flags);
        if (rc)
            fprintf(stderr, "failed to modify QP state to RTR\n");
        return rc;
    }

    int RDMA_Manager::modify_qp_to_rts(struct ibv_qp *qp)
    {
        struct ibv_qp_attr attr;
        int flags;
        int rc;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTS;
        attr.timeout = 0x12;
        attr.retry_cnt = 6;
        attr.rnr_retry = 0;
        attr.sq_psn = 0;
        attr.max_rd_atomic = 1;
        flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
        rc = ibv_modify_qp(qp, &attr, flags);
        if (rc)
            fprintf(stderr, "failed to modify QP state to RTS\n");
        return rc;
    }

    int RDMA_Manager::RDMA_Send()
    {
        int rc = 0;
        char temp_char;
        char temp_send[] = "Q";
        rc = sock_sync_data(res->sock, 1, temp_send, &temp_char);
        if (rc) /* just send a dummy char back and forth */
        {
            fprintf(stderr, "sync error before RDMA_Send post_send\n");
            goto RDMA_Send_exit;
        }

        rc = post_send(IBV_WR_SEND);
        if (rc)
        {
            fprintf(stderr, "failed to RDMA_Send post_send\n");
            goto RDMA_Send_exit;
        }
        /* sync to make sure that both sides are in states that they can connect to prevent packet loose */

        rc = poll_completion();
        if (rc)
        {
            fprintf(stderr, "RDMA_Send poll completion failed\n");
            goto RDMA_Send_exit;
        }
    RDMA_Send_exit:
        return rc;
    }

    int RDMA_Manager::RDMA_Receive()
    {
        int rc = 0;
        char temp_char;
        char temp_send[] = "Q";
        rc = post_receive();
        if (rc)
        {
            fprintf(stderr, "failed to RDMA_Receive post_receive\n");
            goto RDMA_Receive_exit;
        }
        /* sync to make sure that both sides are in states that they can connect to prevent packet loose */

        rc = sock_sync_data(res->sock, 1, temp_send, &temp_char);
        if (rc) /* just send a dummy char back and forth */
        {
            fprintf(stderr, "sync error after RDMA_Receive post_receive\n");
            goto RDMA_Receive_exit;
        }

        rc = poll_completion();
        if (rc)
        {
            fprintf(stderr, "RDMA_Receive poll completion failed\n");
            goto RDMA_Receive_exit;
        }
    RDMA_Receive_exit:
        return rc;
    }

    int RDMA_Manager::connect_qp()
    {
        struct cm_con_data_t local_con_data;
        struct cm_con_data_t remote_con_data;
        struct cm_con_data_t tmp_con_data;
        int rc = 0;
        char temp_char;
        char temp_send[] = "Q";
        union ibv_gid my_gid;
        if (config.gid_idx >= 0)
        {
            rc = ibv_query_gid(res->ib_ctx, config.ib_port, config.gid_idx, &my_gid);
            if (rc)
            {
                fprintf(stderr, "could not get gid for port %d, index %d\n", config.ib_port, config.gid_idx);
                return rc;
            }
        }
        else
            memset(&my_gid, 0, sizeof my_gid);
        /* exchange using TCP sockets info required to connect QPs */
        local_con_data.addr = htonll((uintptr_t)res->buf);
        local_con_data.rkey = htonl(res->mr->rkey);
        local_con_data.qp_num = htonl(res->qp->qp_num);
        local_con_data.lid = htons(res->port_attr.lid);
        memcpy(local_con_data.gid, &my_gid, 16);
        fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
        if (sock_sync_data(res->sock, sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data) < 0)
        {
            fprintf(stderr, "failed to exchange connection data between sides\n");
            rc = 1;
            goto connect_qp_exit;
        }
        remote_con_data.addr = ntohll(tmp_con_data.addr);
        remote_con_data.rkey = ntohl(tmp_con_data.rkey);
        remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
        remote_con_data.lid = ntohs(tmp_con_data.lid);
        memcpy(remote_con_data.gid, tmp_con_data.gid, 16);
        /* save the remote side attributes, we will need it for the post SR */
        res->remote_props = remote_con_data;
        fprintf(stdout, "Remote address = 0x%" PRIx64 "\n", remote_con_data.addr);
        fprintf(stdout, "Remote rkey = 0x%x\n", remote_con_data.rkey);
        fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
        fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data.lid);
        if (config.gid_idx >= 0)
        {
            uint8_t *p = remote_con_data.gid;
            fprintf(stdout, "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ", p[0],
                    p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
        }
        /* modify the QP to init */
        rc = modify_qp_to_init(res->qp);
        if (rc)
        {
            fprintf(stderr, "change QP state to INIT failed\n");
            goto connect_qp_exit;
        }
        // /* let the client post RR to be prepared for incoming messages */

        // rc = post_receive();
        // if (rc)
        // {
        //     fprintf(stderr, "failed to post RR\n");
        //     goto connect_qp_exit;
        // }

        /* modify the QP to RTR */
        rc = modify_qp_to_rtr(res->qp, remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid);
        if (rc)
        {
            fprintf(stderr, "failed to modify QP state to RTR\n");
            goto connect_qp_exit;
        }
        rc = modify_qp_to_rts(res->qp);
        if (rc)
        {
            fprintf(stderr, "failed to modify QP state to RTR\n");
            goto connect_qp_exit;
        }
        fprintf(stdout, "QP state was change to RTS\n");
        // /* sync to make sure that both sides are in states that they can connect to prevent packet loose */

        // if (sock_sync_data(res->sock, 1, temp_send, &temp_char)) /* just send a dummy char back and forth */
        // {
        //     fprintf(stderr, "sync error after QPs are were moved to RTS\n");
        //     rc = 1;
        // }
    connect_qp_exit:
        return rc;
    }

    int RDMA_Manager::RDMA_Read()
    {
        char temp_char;
        char temp_send_R[] = "R";
        if (post_send(IBV_WR_RDMA_READ))
        {
            fprintf(stderr, "failed to RDMA_Read post_send\n");
            return 1;
        }
        if (poll_completion())
        {
            fprintf(stderr, "RDMA_Read poll completion failed\n");
            return 1;
        }
        // if (sock_sync_data(res->sock, 1, temp_send_R, &temp_char)) /* just send a dummy char back and forth */
        // {
        //     fprintf(stderr, "sync error after RDMA ops\n");
        //     return 1;
        // }
        return 0;
    }
    
    int RDMA_Manager::RDMA_Write()
    {
        char temp_char;
        char temp_send_W[] = "W";
        if (post_send(IBV_WR_RDMA_WRITE))
        {
            fprintf(stderr, "failed to RDMA_Write post_send\n");
            return 1;
        }
        if (poll_completion())
        {
            fprintf(stderr, "RDMA_Write poll completion failed 3\n");
            return 1;
        }
        if (sock_sync_data(res->sock, 1, temp_send_W, &temp_char)) /* just send a dummy char back and forth */
        {
            fprintf(stderr, "sync error after RDMA_Write post_send\n");
            return 1;
        }
        return 0;
    }

    int RDMA_Manager::resources_destroy()
    {
        int rc = 0;
        if (res->qp)
            if (ibv_destroy_qp(res->qp))
            {
                fprintf(stderr, "failed to destroy QP\n");
                rc = 1;
            }
        if (res->mr)
            if (ibv_dereg_mr(res->mr))
            {
                fprintf(stderr, "failed to deregister MR\n");
                rc = 1;
            }
        if (res->buf)
            free(res->buf);
        if (res->cq)
            if (ibv_destroy_cq(res->cq))
            {
                fprintf(stderr, "failed to destroy CQ\n");
                rc = 1;
            }
        if (res->pd)
            if (ibv_dealloc_pd(res->pd))
            {
                fprintf(stderr, "failed to deallocate PD\n");
                rc = 1;
            }
        if (res->ib_ctx)
            if (ibv_close_device(res->ib_ctx))
            {
                fprintf(stderr, "failed to close device context\n");
                rc = 1;
            }
        if (res->sock >= 0)
            if (close(res->sock))
            {
                fprintf(stderr, "failed to close socket\n");
                rc = 1;
            }
        return rc;
    }

    void RDMA_Manager::print_config(void)
    {
        fprintf(stdout, " ------------------------------------------------\n");
        fprintf(stdout, " Device name : \"%s\"\n", config.dev_name);
        fprintf(stdout, " IB port : %u\n", config.ib_port);

        fprintf(stdout, " IP : %s\n", config.server_name);
        fprintf(stdout, " TCP port : %u\n", config.tcp_port);
        if (config.gid_idx >= 0)
            fprintf(stdout, " GID index : %u\n", config.gid_idx);
        fprintf(stdout, " ------------------------------------------------\n\n");
    }

    void RDMA_Manager::usage(const char *argv0)
    {
        fprintf(stdout, "Usage:\n");
        fprintf(stdout, " %s start a server and wait for connection\n", argv0);
        fprintf(stdout, " %s <host> connect to server at <host>\n", argv0);
        fprintf(stdout, "\n");
        fprintf(stdout, "Options:\n");
        fprintf(stdout, " -p, --port <port> listen on/connect to port <port> (default 18515)\n");
        fprintf(stdout, " -d, --ib-dev <dev> use IB device <dev> (default first device found)\n");
        fprintf(stdout, " -i, --ib-port <port> use port <port> of IB device (default 1)\n");
        fprintf(stdout, " -g, --gid_idx <git index> gid index to be used in GRH (default not used)\n");
    }

    int RDMA_Manager::init(config_t config)
    {
        this->config=config;
        res = new resources();
        memset(res, 0, sizeof(*res));
        res->sock = -1;

        print_config();
        if (resources_create())
        {
            fprintf(stderr, "failed to create resources\n");
            return 1;
        }
        /* connect the QPs */
        if (connect_qp())
        {
            fprintf(stderr, "failed to connect QPs\n");
            return 1;
        }
        return 0;
    }
}