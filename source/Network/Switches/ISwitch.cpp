#include "ISwitch.hpp"

#include "spdlog/spdlog.h"

namespace Network::Switches {
static bool bNetworkComputing = true;

void setNetworkComputing(const bool enable)
{
    static bool bCalled = false;

    if(bCalled) {
        throw "Cannot change network computing capabilities after it has been set!";
    }
    else {
        bCalled           = true;
        bNetworkComputing = enable;
    }
}

bool isNetworkComputingEnabled()
{
    return bNetworkComputing;
}

ISwitch::ISwitch(const size_t ID, const size_t portAmount)
: m_ID(ID), m_portAmount(portAmount), m_ports(m_portAmount)
{
    if(4 > m_portAmount) {
        spdlog::error("Port amount({}) cannot be smaller than 4!", m_portAmount);

        throw "Invalid port amount!";
    }

    if(0 != (m_portAmount % 2)) {
        spdlog::error("Port amount({}) must be an exact multiple of 2!", m_portAmount);

        throw "Invalid port amount!";
    }
}

Network::Port &ISwitch::getPort(const size_t &portID)
{
    if(portID >= m_ports.size()) {
        spdlog::error("Switch doesn't have a port with ID {}", portID);

        throw "Invalid port ID!";
    }

    return m_ports.at(portID);
}

bool ISwitch::isReady() const
{
    return std::all_of(m_ports.cbegin(), m_ports.cend(), [](const Port &port) { return port.isConnected(); });
}

bool ISwitch::canCompute() const
{
    return bNetworkComputing;
}
}; // namespace Network::Switches
