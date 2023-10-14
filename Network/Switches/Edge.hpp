#pragma  once

#include "ISwitch.hpp"
#include <vector>
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

    private: /** Members **/
        std::map<std::size_t, Port&> m_downPortTable; // Re-direction table for down-ports

        inline static std::size_t nextID = 0; // i.e. Number of edge switches in total
    };
}
