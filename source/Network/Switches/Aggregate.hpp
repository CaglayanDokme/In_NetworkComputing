#pragma  once

#include "InterSwitchMessages.hpp"
#include "Network/Message.hpp"
#include "ISwitch.hpp"
#include <map>

namespace Network::Switches {
    class Aggregate : public ISwitch {
    public: /** Construction **/
        Aggregate() = delete;
        explicit  Aggregate(const size_t portAmount);

        // Forbid copying
        Aggregate(const Aggregate &) = delete;
        Aggregate &operator=(const Aggregate &) = delete;

        // Must be move-able to store in containers
        Aggregate(Aggregate &&) = default;
        Aggregate &operator=(Aggregate &&) = delete;

    public: /** Methods **/
        [[nodiscard]] bool tick() final;

        /**
         * @brief  Get reference to a specific up-port of this switch
         * @param  portID ID of the up-port to get (Starting from 0)
         * @return Reference to requested port object
         *
         * @note Up-port #0 is connected to the core switch with smallest ID
         */
        [[nodiscard]] Port &getUpPort(const size_t &portID);

        /**
         * @brief  Get reference to a specific down-port of this switch
         * @param  portID ID of the down-port to get (Starting from 0)
         * @return Reference to requested port object
         *
         * @note Down-port #0 is connected to the edge switch with smallest ID
         */
        [[nodiscard]] Port &getDownPort(const size_t &portID);

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

        /**
         * @brief  Find the up-port with minimum messages to be sent (i.e. minimum potential delay)
         * @return Reference to the most available port
         */
        [[nodiscard]] Port &getAvailableUpPort();

        /**
         * @brief  Get number of up/down ports
         * @return Amount of requested port type
         */
        [[nodiscard]] size_t getDownPortAmount() const;
        [[nodiscard]] size_t getUpPortAmount() const;

    private: /** Members **/
        const size_t assocCompNodeAmount;
        const size_t firstCompNodeIdx;
        size_t m_nextPort{0};
        std::map<size_t, Port&> m_downPortTable; // Re-direction table for down-ports
        size_t m_subColumnIdx;                   // Index of the sub-column this aggregate switch belongs to (i.e. index of the column in group)
        size_t m_sameColumnPortID;               // ID of the same-column down-port

        std::map<size_t, bool> m_barrierRequestFlags; // Key: Down-port index, Value: True/False
        std::map<size_t, bool> m_barrierReleaseFlags; // Key: Computation node index, Value: True/False

        struct {
            std::vector<size_t> contributors;                 // Computing node IDs that contributed to the reduction
            size_t destinationID;                             // ID of the destined computing node (i.e. root process of reduce operation)
            Messages::Reduce::OpType opType;                  // Current operation type
            decltype(Messages::Reduce::m_data) value;         // Current reduction value (e.g. Sum of received values, maximum of received values)
        } m_reduceState; // We only need state for messages directed to down-ports, up-port destined messages are redirected immediately

        struct {
            struct {
                bool bOngoing{false};                        // True if a reduce operation is ongoing
                std::map<size_t, bool> receiveFlags;    // Key: Port index (Only down-ports or only up-ports), Value: True/False
                Messages::ReduceAll::OpType opType;          // Current operation type
                decltype(Messages::ReduceAll::m_data) value; // Current reduction value (e.g. Sum of received values, maximum of received values)
            } toUp, toDown;
        } m_reduceAllStates;

        struct {
            std::vector<
                std::pair<
                    size_t,            // Source computing node ID
                    std::vector<float> // Data to be gathered
                >
            > value;

            size_t destinationID; // ID of the destined computing node (i.e. root process of gather operation)
        } m_gatherState; // We only need state for messages directed to down-ports, up-port destined messages are redirected immediately

        struct {
            struct {
                bool bOngoing{false};                                       // True if an all-gather operation is ongoing
                std::map<size_t, bool> receiveFlags;                   // Key: Port index (Only down-ports or only up-ports), Value: True/False
                decltype(Messages::InterSwitch::AllGather::m_data) value;   // Current all-gather value
            } toUp, toDown;
        } m_allGatherStates;

        inline static size_t nextID = 0; // i.e. Number of aggregate switches in total
    };
}
