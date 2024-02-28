#pragma once

#include <cstddef>
#include <cmath>

namespace Network::Utilities {
    [[nodiscard]] inline std::size_t deriveCoreSwitchAmount(const std::size_t portPerSwitch)
    {
        static const auto coreSwitchAmount = static_cast<std::size_t>(std::pow(portPerSwitch, 2) / 4);

        return  coreSwitchAmount;
    }

    [[nodiscard]] inline std::size_t deriveAggregateSwitchAmount(const std::size_t portPerSwitch)
    {
        static const auto aggregateSwitchAmount = deriveCoreSwitchAmount(portPerSwitch) * 2;

        return  aggregateSwitchAmount;
    }

    [[nodiscard]] inline std::size_t deriveEdgeSwitchAmount(const std::size_t portPerSwitch)
    {
        static const auto edgeSwitchAmount = deriveCoreSwitchAmount(portPerSwitch) * 2;;

        return  edgeSwitchAmount;
    }

    [[nodiscard]] inline std::size_t deriveComputingNodeAmount(const std::size_t portPerSwitch)
    {
        static const auto compNodeAmount = deriveEdgeSwitchAmount(portPerSwitch) * (portPerSwitch / 2);

        return  compNodeAmount;
    }
}
