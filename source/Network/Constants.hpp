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

    /**
     * @brief  Get the number of groups in the network
     * @return The number of groups
     *
     * @note A group consists of aggregate switches and edge switches that are connected to each other
     */
    [[nodiscard]] size_t getGroupAmount();

    /**
     * @brief  Get the number of columns in the network
     * @return The number of columns
     *
     * @note A column consists of an aggregate switch and edge switches that are placed in the same vertical line
     */
    [[nodiscard]] size_t getColumnAmount();

    /**
     * @brief  Get the number of columns in a group
     * @return The number of columns in a group
     */
    [[nodiscard]] size_t getSubColumnAmountPerGroup();

    /**
     * @brief  Get the column index of a computing node
     * @param  compNodeIdx Index of the computing node
     * @return The column index of a computing node
     */
    [[nodiscard]] size_t getColumnIdxOfCompNode(const size_t compNodeIdx);

    /**
     * @brief  Get the sub-column index of a computing node
     * @param  compNodeIdx Index of the computing node
     * @return The sub-column index of a computing node
     */
    [[nodiscard]] size_t getSubColumnIdxOfCompNode(const size_t compNodeIdx);
}
