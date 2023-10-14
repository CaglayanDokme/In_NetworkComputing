#include "Aggregate.hpp"
#include "spdlog/spdlog.h"

using namespace Network::Switches;

Aggregate::Aggregate(const std::size_t portAmount)
: ISwitch(nextID++, portAmount)
{
    spdlog::trace("Created aggregate switch with ID #{}", m_ID);

    // TODO Log anticipated computing node IDs
}

bool Aggregate::tick()
{
    return false;
}

Network::Port &Aggregate::getUpPort(const size_t &portID)
{
    static const std::size_t upPortAmount = m_portAmount / 2;

    if(portID >= upPortAmount) {
        spdlog::error("Switch doesn't have an up-port with ID {}", portID);

        throw "Invalid up-port ID!";
    }

    return getPort(portID);
}

Network::Port &Aggregate::getDownPort(const size_t &portID)
{
    static const std::size_t downPortAmount = m_portAmount / 2;

    if(portID >= downPortAmount) {
        spdlog::error("Switch doesn't have a down-port with ID {}", portID);

        throw "Invalid down-port ID!";
    }

    return getPort((m_portAmount / 2) + portID);
}
