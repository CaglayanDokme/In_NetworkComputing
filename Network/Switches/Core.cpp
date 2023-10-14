#include "Core.hpp"
#include "spdlog/spdlog.h"

using namespace Network::Switches;

Core::Core(const std::size_t portAmount)
: ISwitch(nextID++, portAmount)
{
    spdlog::trace("Created core switch with ID #{}", m_ID);
}

bool Core::tick()
{
    // Advance all ports
    for(auto &port : m_ports) {
        port.tick();
    }

    // Check all ports for incoming messages
    static const auto compNodePerPort = std::size_t(std::pow(m_portAmount / 2, 2));
    for(size_t portIdx = 0; portIdx < m_ports.size(); ++portIdx) {
        auto &sourcePort = m_ports.at(portIdx);

        if(!sourcePort.hasIncoming()) {
            continue;
        }

        const auto msg = sourcePort.popIncoming();
        spdlog::trace("Core Switch({}): Message received from sourcePort #{} destined to computing node #{}.", m_ID, portIdx, msg.m_destinationID);

        const auto targetPortIdx = msg.m_destinationID / compNodePerPort;
        spdlog::trace("Core Switch({}): Re-directing to port #{}..", targetPortIdx);

        m_ports.at(targetPortIdx).pushOutgoing(msg);

        if(portIdx == targetPortIdx) {
            spdlog::warn("Core Switch({}): Target and source ports are the same({})!", m_ID, portIdx);
        }
    }

    return true;
}
