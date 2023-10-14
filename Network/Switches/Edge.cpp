#include "Edge.hpp"
#include "spdlog/spdlog.h"

using namespace Network::Switches;

Edge::Edge(const std::size_t portAmount)
: ISwitch(nextID, portAmount)
{
    spdlog::trace("Created edge switch with ID #{}", m_ID);

    // TODO Log anticipated computing node IDs
}

bool Edge::tick()
{
    return false;
}

Network::Port &Edge::getUpPort(const size_t &portID)
{
    static const std::size_t upPortAmount = m_portAmount / 2;

    if(portID >= upPortAmount) {
        spdlog::error("Switch doesn't have an up-port with ID {}", portID);

        throw "Invalid up-port ID!";
    }

    return getPort(portID);
}

Network::Port &Edge::getDownPort(const size_t &portID)
{
    static const std::size_t downPortAmount = m_portAmount / 2;

    if(portID >= downPortAmount) {
        spdlog::error("Switch doesn't have a down-port with ID {}", portID);

        throw "Invalid down-port ID!";
    }

    return getPort((m_portAmount / 2) + portID);
}

