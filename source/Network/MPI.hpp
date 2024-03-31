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

    private:
        enum class State {
            Idle,
            Acknowledge,
            Receive,
            BroadcastReceive,
            Barrier,
            Reduce,
            ReduceAll
        };

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

    private:
        void setState(const State state);

    private: /** Members **/
        const std::size_t m_ID;
        State m_state{State::Idle};
        Port m_port;

        // Acknowledge
        struct {
            size_t sourceID{0};
            Messages::e_Type ackType;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_acknowledge;

        // Direct receive
        struct {
            decltype(Messages::DirectMessage::m_data) receivedData;
            size_t sourceID{0};
            std::mutex mutex;
            std::condition_variable notifier;
        } m_directReceive;

        // Broadcast receive
        struct {
            decltype(Messages::BroadcastMessage::m_data) receivedData;
            size_t sourceID{0};
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
            decltype(Messages::Reduce::m_data) receivedData;
            ReduceOp operation;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_reduce;

        // Reduce all
        struct {
            decltype(Messages::ReduceAll::m_data) receivedData{0.0f};
            ReduceOp operation;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_reduceAll;
    };
};
