#pragma  once

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

        inline static std::size_t nextID = 0; // i.e. Number of edge switches in total
    };
}
