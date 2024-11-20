#pragma once

#include "InterSwitchMessages.hpp"
#include "Network/Message.hpp"
#include "ISwitch.hpp"
#include <map>

namespace Network::Switches {
    class Core : public ISwitch {
    public: /** Construction **/
        Core() = delete;
        explicit  Core(const size_t portAmount);

        // Forbid copying
        Core(const Core &) = delete;
        Core &operator=(const Core &) = delete;

        // Must be move-able to store in containers
        Core(Core &&) = default;
        Core &operator=(Core &&) = delete;

    public: /** Methods **/
        [[nodiscard]] bool tick() final;

    private:
        /**
         * @brief Process a message received from a port
         * @param sourcePortIdx Index of the source port
         * @param msg          Message to be processed
         */
        void process(const size_t sourcePortIdx, std::unique_ptr<Messages::DirectMessage> msg);
        void process(const size_t sourcePortIdx, std::unique_ptr<Messages::Acknowledge> msg);
        void process(const size_t sourcePortIdx, std::unique_ptr<Messages::BroadcastMessage> msg);
        void process(const size_t sourcePortIdx, std::unique_ptr<Messages::BarrierRequest> msg);
        void process(const size_t sourcePortIdx, std::unique_ptr<Messages::BarrierRelease> msg);
        void process(const size_t sourcePortIdx, std::unique_ptr<Messages::ReduceAll> msg);
        void process(const size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Reduce> msg);
        void process(const size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Scatter> msg);
        void process(const size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Gather> msg);
        void process(const size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::AllGather> msg);

        /**
         * @brief Redirect a message to its destination
         * @param sourcePortIdx Index of the source port
         * @param msg Message to be redirected
         */
        void redirect(const size_t sourcePortIdx, Network::Port::UniqueMsg msg);

    private: /** Members **/
        size_t m_nextPort{0};
        std::map<size_t, bool> m_barrierRequestFlags; // Key: Port index, Value: True/False

        struct {
            std::map<size_t, bool> flags;        // Key: Port index, Value: True/False
            size_t destinationID;                // ID of the destined computing node (i.e. root process of reduce operation)
            size_t destinationPortID;            // Port index of the destined computing node
            Messages::Reduce::OpType opType;          // Current operation type
            decltype(Messages::Reduce::m_data) value; // Current reduction value (e.g. Sum of received values, maximum of received values)
        } m_reduceStates;

        struct {
            bool bOngoing{false};                        // True if a reduce-all operation is ongoing
            std::map<size_t, bool> flags;           // Key: Port index, Value: True/False
            Messages::ReduceAll::OpType opType;          // Current operation type
            decltype(Messages::ReduceAll::m_data) value; // Current reduction value (e.g. Sum of received values, maximum of received values)
        } m_reduceAllStates;

        struct {
            bool bOngoing{false};                        // True if an all-gather operation is ongoing
            std::map<size_t, bool> flags;           // Key: Port index, Value: True/False
            decltype(Messages::InterSwitch::AllGather::m_data) value; // Current all-gather value
        } m_allGatherStates;

        inline static size_t nextID = 0; // i.e. Number of core switches in total
        inline static size_t compNodePerPort = 0;
    };
}
