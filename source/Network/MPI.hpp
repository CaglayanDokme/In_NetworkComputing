#pragma once

#include <condition_variable>
#include "Network/Port.hpp"
#include <mutex>

namespace Network {
    class MPI {
        enum class State {
            Idle,
            Receive
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

        void send(const float &data, const size_t destinationID);
        void receive(float &data, const size_t sourceID);

    private:
        void setState(const State state);

    private: /** Members **/
        const std::size_t m_ID;
        State m_state{State::Idle};
        Port m_port;

        // Direct receive
        struct {
            float receivedData{0.0f};
            size_t sourceID{0};
            std::mutex mutex;
            std::condition_variable notifier;
        } m_directReceive;
    };
};
