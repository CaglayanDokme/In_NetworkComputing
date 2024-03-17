#pragma once

#include <cstddef>
#include <cmath>
#include <stdexcept>

namespace Network::Utilities {
    /**
     * @brief  Derive the amount of core switches in the network
     * @param  portPerSwitch The amount of ports per switch (Empty to get the already calculated value)
     * @return The amount of core switches
     */
    [[nodiscard]] inline std::size_t deriveCoreSwitchAmount(const std::size_t portPerSwitch = 0)
    {
        static const auto coreSwitchAmount = static_cast<std::size_t>(std::pow(portPerSwitch, 2) / 4);

        if(0 == coreSwitchAmount) {
            throw std::runtime_error("Core switch amount must be greater than 0");
        }

        return  coreSwitchAmount;
    }

    /**
     * @brief  Derive the amount of aggregate switches in the network
     * @param  portPerSwitch The amount of ports per switch (Empty to get the already calculated value)
     * @return The amount of aggregate switches
     */
    [[nodiscard]] inline std::size_t deriveAggregateSwitchAmount(const std::size_t portPerSwitch = 0)
    {
        static const auto aggregateSwitchAmount = deriveCoreSwitchAmount(portPerSwitch) * 2;

        if(0 == aggregateSwitchAmount) {
            throw std::runtime_error("Aggregate switch amount must be greater than 0");
        }

        return  aggregateSwitchAmount;
    }

    /**
     * @brief  Derive the amount of edge switches in the network
     * @param  portPerSwitch The amount of ports per switch (Empty to get the already calculated value)
     * @return The amount of edge switches
     */
    [[nodiscard]] inline std::size_t deriveEdgeSwitchAmount(const std::size_t portPerSwitch = 0)
    {
        static const auto edgeSwitchAmount = deriveCoreSwitchAmount(portPerSwitch) * 2;

        if(0 == edgeSwitchAmount) {
            throw std::runtime_error("Edge switch amount must be greater than 0");
        }

        return  edgeSwitchAmount;
    }

    /**
     * @brief  Derive the amount of computing nodes in the network
     * @param  portPerSwitch The amount of ports per switch (Empty to get the already calculated value)
     * @return The amount of computing nodes
     */
    [[nodiscard]] inline std::size_t deriveComputingNodeAmount(const std::size_t portPerSwitch = 0)
    {
        static const auto compNodeAmount = deriveEdgeSwitchAmount(portPerSwitch) * (portPerSwitch / 2);

        if(0 == compNodeAmount) {
            throw std::runtime_error("Computing node amount must be greater than 0");
        }

        return  compNodeAmount;
    }
}
