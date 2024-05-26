#pragma once

#include "InterSwitchMessages.hpp"
#include "Network/Message.hpp"
#include "ISwitch.hpp"
#include <map>

namespace Network::Switches {
    class Core : public ISwitch {
    public: /** Construction **/
        Core() = delete;
        explicit  Core(const std::size_t portAmount);

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
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::DirectMessage> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Acknowledge> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::BroadcastMessage> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::BarrierRequest> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::BarrierRelease> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Reduce> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::ReduceAll> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Scatter> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Gather> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::AllGather> msg);

        /**
         * @brief Redirect a message to its destination
         * @param sourcePortIdx Index of the source port
         * @param msg Message to be redirected
         */
        void redirect(const std::size_t sourcePortIdx, Network::Port::UniqueMsg msg);

    private: /** Members **/
        std::map<std::size_t, bool> m_barrierRequestFlags; // Key: Port index, Value: True/False

        struct {
            std::map<std::size_t, bool> flags;        // Key: Port index, Value: True/False
            std::size_t destinationID;                // ID of the destined computing node (i.e. root process of reduce operation)
            std::size_t destinationPortID;            // Port index of the destined computing node
            Messages::Reduce::OpType opType;          // Current operation type
            decltype(Messages::Reduce::m_data) value; // Current reduction value (e.g. Sum of received values, maximum of received values)
        } m_reduceStates;

        struct {
            bool bOngoing{false};                        // True if a reduce-all operation is ongoing
            std::map<std::size_t, bool> flags;           // Key: Port index, Value: True/False
            Messages::ReduceAll::OpType opType;          // Current operation type
            decltype(Messages::ReduceAll::m_data) value; // Current reduction value (e.g. Sum of received values, maximum of received values)
        } m_reduceAllStates;

        struct {
            bool bOngoing{false};                        // True if an all-gather operation is ongoing
            std::map<std::size_t, bool> flags;           // Key: Port index, Value: True/False
            decltype(Messages::InterSwitch::AllGather::m_data) value; // Current all-gather value
        } m_allGatherStates;

        inline static std::size_t nextID = 0; // i.e. Number of core switches in total
        inline static std::size_t compNodePerPort = 0;
    };
}
