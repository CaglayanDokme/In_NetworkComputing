#pragma  once

#include "InterSwitchMessages.hpp"
#include "Network/Message.hpp"
#include "ISwitch.hpp"
#include <map>

namespace Network::Switches {
    class Edge : public ISwitch {
    public: /** Construction **/
        Edge() = delete;
        explicit  Edge(const std::size_t portAmount);

        // Forbid copying
        Edge(const Edge &) = delete;
        Edge &operator=(const Edge &) = delete;

        // Must be move-able to store in containers
        Edge(Edge &&) = default;
        Edge &operator=(Edge &&) = delete;

    public: /** Methods **/
        [[nodiscard]] bool tick() final;

        /**
         * @brief  Get reference to a specific up-port of this switch
         * @param  portID ID of the up-port to get (Starting from 0)
         * @return Reference to requested port object
         *
         * @note Up-port #0 is connected to the aggregate switch with smallest ID
         */
        [[nodiscard]] Port &getUpPort(const std::size_t &portID);

        /**
         * @brief  Get reference to a specific down-port of this switch
         * @param  portID ID of the down-port to get (Starting from 0)
         * @return Reference to requested port object
         *
         * @note Down-port #0 is connected to the computing node with smallest ID
         */
        [[nodiscard]] Port &getDownPort(const std::size_t &portID);

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
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Scatter> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Gather> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::AllGather> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Scatter> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Gather> msg);
        void process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::AllGather> msg);

        /**
         * @brief Redirect a message to its destination
         * @param sourcePortIdx Index of the source port
         * @param msg Message to be redirected
         */
        void redirect(const std::size_t sourcePortIdx, Network::Port::UniqueMsg msg);

        /**
         * @brief  Find the up-port with minimum messages to be sent (i.e. minimum potential delay)
         * @return Reference to the most available port
         */
        [[nodiscard]] Port &getAvailableUpPort();

        /**
         * @brief  Get number of up/down ports
         * @return Amount of requested port type
         */
        [[nodiscard]] std::size_t getDownPortAmount() const;
        [[nodiscard]] std::size_t getUpPortAmount() const;

    private: /** Members **/
        const std::size_t firstCompNodeIdx;                 // Index of the first computing node connected to this switch
        std::map<std::size_t, Port&> m_downPortTable;       // Re-direction table for down-ports
        std::map<std::size_t, bool> m_barrierReleaseFlags;  // Key: Up-port index, Value: True/False

        struct {
            struct {
                bool bOngoing{false};                     // True if a reduce operation is ongoing
                std::map<std::size_t, bool> receiveFlags; // Key: Port index, Value: True/False
                std::size_t destinationID;                // ID of the destined computing node (i.e. root process of reduce operation)
                Messages::Reduce::OpType opType;          // Current operation type
                decltype(Messages::Reduce::m_data) value; // Current reduction value (e.g. Sum of received values, maximum of received values)
            } toUp, toDown;

            std::size_t sameColumnPortID; // ID of the same-column up-port
        } m_reduceStates;

        struct {
            struct {
                bool bOngoing{false};                        // True if a reduce operation is ongoing
                std::map<std::size_t, bool> receiveFlags;    // Key: Port index (Only down-ports or only up-ports), Value: True/False
                Messages::ReduceAll::OpType opType;          // Current operation type
                decltype(Messages::ReduceAll::m_data) value; // Current reduction value (e.g. Sum of received values, maximum of received values)
            } toUp, toDown;
        } m_reduceAllStates;

        struct GatherState {
            struct {
                bool bOngoing{false};                                  // True if a gather operation is ongoing
                std::size_t destinationID;                             // ID of the destined computing node (i.e. root process of gather operation)
                decltype(Messages::InterSwitch::Gather::m_data) value; // Current gathered value
                std::size_t refSize;                                    // Expected size of the gathered data
            } toUp;

            struct ToDown {
                bool bOngoing{false};                                   // True if a gather operation is ongoing
                std::size_t destinationID;                              // ID of the destined computing node (i.e. root process of gather operation)
                std::vector<decltype(Messages::Gather::m_data)> value;  // Current gathered value
                std::size_t refSize;                                    // Expected size of the gathered data

                /**
                 * @brief Push data to the gather buffer
                 * @param compNodeIdx Index of the computing node that sent the data
                 * @param destID      ID of the destined computing node
                 * @param data        Data to be gathered
                 * @return True if the data is ready to be redirected to the destined computing node
                 */
                [[nodiscard]] bool push(const std::size_t compNodeIdx, const std::size_t destID, decltype(Messages::Gather::m_data) &&data);

                /**
                 * @brief Reset the gather buffer to initial state
                 */
                void reset();
            } toDown;
        } m_gatherStates;

        struct AllGatherState {
            struct {
                bool bOngoing{false};                                     // True if an all-gather operation is ongoing
                decltype(Messages::InterSwitch::AllGather::m_data) value; // Current gathered value
            } toUp;

            struct {
                bool bOngoing{false};                                                   // True if an all-gather operation is ongoing
                std::map<std::size_t, bool> receiveFlags;                               // Key: Port index(Up-port), Value: True/False
                decltype(Messages::InterSwitch::AllGather::m_data) value;  // Current gathered value
            } toDown;
        } m_allGatherStates;

        inline static std::size_t nextID = 0; // i.e. Number of edge switches in total
    };
}
