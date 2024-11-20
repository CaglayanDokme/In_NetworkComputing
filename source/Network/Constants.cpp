#include "Constants.hpp"

#include <cmath>
#include <stdexcept>
#include <optional>

namespace Network::Constants {
    static std::optional<size_t> portPerSwitch;

    void setPortPerSwitch(const size_t value)
    {
        if(portPerSwitch.has_value()) {
            throw std::logic_error("Port per switch amount has already been set!");
        }

        portPerSwitch = value;
    }

    size_t getPortPerSwitch()
    {
        if(!portPerSwitch.has_value()) {
            throw std::runtime_error("Port per switch amount must be set first!");
        }

        return portPerSwitch.value();
    }

    size_t deriveCoreSwitchAmount()
    {
        static const auto coreSwitchAmount = static_cast<size_t>(std::pow(getPortPerSwitch(), 2) / 4);

        if(0 == coreSwitchAmount) {
            throw std::runtime_error("Core switch amount must be greater than 0");
        }

        return  coreSwitchAmount;
    }

    size_t deriveAggregateSwitchAmount()
    {
        static const auto aggregateSwitchAmount = deriveCoreSwitchAmount() * 2;

        return  aggregateSwitchAmount;
    }

    size_t deriveEdgeSwitchAmount()
    {
        static const auto edgeSwitchAmount = deriveCoreSwitchAmount() * 2;

        return  edgeSwitchAmount;
    }

    size_t deriveComputingNodeAmount()
    {
        static const auto compNodeAmount = deriveEdgeSwitchAmount() * (getPortPerSwitch() / 2);

        return  compNodeAmount;
    }

    size_t getGroupAmount()
    {
        static const auto groupAmount = getPortPerSwitch();

        return  groupAmount;
    }

    size_t getColumnAmount()
    {
        static const auto columnAmount = deriveEdgeSwitchAmount();

        return  columnAmount;
    }

    size_t getSubColumnAmountPerGroup()
    {
        static const auto subColumnAmount = getColumnAmount() / getGroupAmount();

        return  subColumnAmount;
    }

    size_t getColumnIdxOfCompNode(const size_t compNodeIdx)
    {
        static const auto compNodePerColumn = deriveComputingNodeAmount() / getColumnAmount();

        return compNodeIdx / compNodePerColumn;
    }

    size_t getSubColumnIdxOfCompNode(const size_t compNodeIdx)
    {
        const auto columnIdx = getColumnIdxOfCompNode(compNodeIdx);

        return columnIdx % getSubColumnAmountPerGroup();
    }

}