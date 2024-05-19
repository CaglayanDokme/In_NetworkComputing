/**
 * @file    MPI.hpp
 * @brief   Declaration of the MPI class which stands for the computing and message passing interface of the network.
 * @author  Caglayan Dokme (caglayan.dokme@plan.space)
 * @date    March 17, 2024
 */

#pragma once

#include <condition_variable>
#include "Network/Port.hpp"
#include "Message.hpp"
#include <vector>
#include <mutex>

namespace Network {
    class MPI {
    public: /** Types **/
    public:
        using ReduceOp = Messages::ReduceOperation;

    public: /** Construction **/
        MPI(const std::size_t ID);

        // Forbid copying
        MPI(const MPI &) = delete;
        MPI &operator=(const MPI &) = delete;

        // Forbid moving
        MPI(MPI &&) = delete;
        MPI &operator=(MPI &&) = delete;

    public: /** Methods **/
        void tick();

        /**
         * @brief  Get the port of the computing node
         * @return Reference to the contained port
         */
        [[nodiscard]] Port &getPort() { return m_port; }

        /**
         * @brief  Check if the computing node has been initialized properly
         * @return True If the connection port is connected to another port
         */
        [[nodiscard]] bool isReady() const;

        /**
         * @brief  Send a message to another computing node
         * @param  data The data(array) to be sent
         * @param  destinationID The ID of the destination computing node
         */
        void send(const std::vector<float> &data, const size_t destinationID);

        /**
         * @brief  Receive a message from another computing node
         * @param  data The data(array) to be received
         * @param  sourceID The ID of the source computing node
         *
         * @note The destination data(array) must be empty
         */
        void receive(std::vector<float> &data, const size_t sourceID);

        /**
         * @brief Broadcast a message to all computing nodes or receive a broadcasted message
         * @param data The data(array) to be broadcasted or to be filled with the received data
         * @param sourceID The ID of the broadcaster node
         *
         * @note If receiving, the destination data(array) must be empty
         */
        void broadcast(std::vector<float> &data, const size_t sourceID);

        /**
         * @brief Get blocked until all computing nodes reach the barrier
         */
        void barrier();

        /**
         * @brief Reduce the data of all computing nodes to a single node
         * @param data          The data(array) to be reduced
         * @param operation     The operation to be applied during the reduction
         * @param destinationID The ID of the destination computing node
         *
         * @note The destination node must also contribute to the reduction by providing data(array)
         */
        void reduce(std::vector<float> &data, const ReduceOp operation, const size_t destinationID);

        /**
         * @brief Reduce the data of all computing nodes to all nodes
         * @param data      The data(array) to be reduced
         * @param operation The operation to be applied during the reduction
         */
        void reduceAll(std::vector<float> &data, const ReduceOp operation);

        /**
         * @brief Scatter the data of a single computing node to all computing nodes
         * @param data     The data array to be scattered
         * @param sourceID The ID of the source computing node
         *
         * @note If not the source node, the destination data(array) must be empty
         */
        void scatter(std::vector<float> &data, const size_t sourceID);

        /**
         * @brief Gather the data of all computing nodes to a single node
         * @param data          The data(array) to be gathered
         * @param destinationID The ID of the destination computing node
         *
         * @note The destination node must also contribute to the gathering by providing data(array)
         */
        void gather(std::vector<float> &data, const size_t destinationID);

        /**
         * @brief Gather the data of all computing nodes at all nodes.
         * @param data The data(array) to be gathered
         *
         * @note The destination data(array) must not be empty
         */
        void allGather(std::vector<float> &data);

    private: /** Members **/
        const std::size_t m_ID;
        Port m_port;

        // Acknowledge
        struct {
            std::deque<std::unique_ptr<Messages::Acknowledge>> messages;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_acknowledge;

        // Direct receive
        struct {
            std::deque<std::unique_ptr<Messages::DirectMessage>> messages;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_directReceive;

        // Broadcast receive
        struct {
            std::deque<std::unique_ptr<Messages::BroadcastMessage>> messages;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_broadcastReceive;

        // Barrier
        struct {
            std::mutex mutex;
            std::condition_variable notifier;
        } m_barrier;

        // Reduce
        struct {
            std::deque<std::unique_ptr<Messages::Reduce>> messages;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_reduce;

        // Reduce all
        struct {
            std::deque<std::unique_ptr<Messages::ReduceAll>> messages;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_reduceAll;

        // Scatter
        struct {
            std::deque<std::unique_ptr<Messages::Scatter>> messages;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_scatter;

        // Gather
        struct {
            std::deque<std::unique_ptr<Messages::Gather>> messages;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_gather;

        // All gather
        struct {
            std::deque<std::unique_ptr<Messages::AllGather>> messages;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_allGather;
    };
};
