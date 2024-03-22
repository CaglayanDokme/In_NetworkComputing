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

        void send(const std::vector<float> &data, const size_t destinationID);
        void receive(std::vector<float> &data, const size_t sourceID);
        void broadcast(float &data, const size_t sourceID);
        void barrier();
        void reduce(float &data, const ReduceOp operation, const size_t destinationID);
        void reduceAll(float &data, const ReduceOp operation);

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
            std::vector<float> receivedData;
            size_t sourceID{0};
            std::mutex mutex;
            std::condition_variable notifier;
        } m_directReceive;

        // Broadcast receive
        struct {
            float receivedData{0.0f};
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
            float receivedData{0.0f};
            ReduceOp operation;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_reduce;

        // Reduce all
        struct {
            float receivedData{0.0f};
            ReduceOp operation;
            std::mutex mutex;
            std::condition_variable notifier;
        } m_reduceAll;
    };
};
