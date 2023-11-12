#pragma  once

#include "ISwitch.hpp"
#include <vector>
#include <map>

namespace Network::Switches {
    class Aggregate : public ISwitch {
    public: /** Construction **/
        Aggregate() = delete;
        explicit  Aggregate(const std::size_t portAmount);

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
        [[nodiscard]] Port &getUpPort(const std::size_t &portID);

        /**
         * @brief  Get reference to a specific down-port of this switch
         * @param  portID ID of the down-port to get (Starting from 0)
         * @return Reference to requested port object
         *
         * @note Down-port #0 is connected to the edge switch with smallest ID
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
        std::map<std::size_t, Port&> m_downPortTable; // Re-direction table for down-ports

        inline static std::size_t nextID = 0; // i.e. Number of aggregate switches in total
    };
}
