#pragma once

#include <cstddef>

namespace Network::Constants {
    /**
     * @brief  Set the amount of ports per switch for once
     * @param  portPerSwitch The amount of ports per switch
     */
    void setPortPerSwitch(const size_t portPerSwitch);

    /**
     * @brief  Get the amount of ports per switch
     * @return The amount of ports per switch
     */
    [[nodiscard]] size_t getPortPerSwitch();

    /**
     * @brief  Derive the amount of core switches in the network
     * @return The amount of core switches
     */
    [[nodiscard]] size_t deriveCoreSwitchAmount();

    /**
     * @brief  Derive the amount of aggregate switches in the network
     * @return The amount of aggregate switches
     */
    [[nodiscard]] size_t deriveAggregateSwitchAmount();

    /**
     * @brief  Derive the amount of edge switches in the network
     * @return The amount of edge switches
     */
    [[nodiscard]] size_t deriveEdgeSwitchAmount();

    /**
     * @brief  Derive the amount of computing nodes in the network
     * @return The amount of computing nodes
     */
    [[nodiscard]] size_t deriveComputingNodeAmount();
}
