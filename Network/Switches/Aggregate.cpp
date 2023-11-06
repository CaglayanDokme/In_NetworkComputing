#include "Aggregate.hpp"
#include "spdlog/spdlog.h"
#include "Network/Message.hpp"

using namespace Network::Switches;

Aggregate::Aggregate(const std::size_t portAmount)
: ISwitch(nextID++, portAmount)
{
    spdlog::trace("Created aggregate switch with ID #{}", m_ID);

    // Calculate look-up table for re-direction to down ports
    {
        const std::size_t downPortAmount        = m_portAmount / 2;
        const std::size_t assocCompNodeAmount   = downPortAmount * downPortAmount;
        const std::size_t firstCompNodeIdx      = (m_ID / downPortAmount) * assocCompNodeAmount;

        for(size_t downPortIdx = 0; downPortIdx < downPortAmount; ++downPortIdx) {
            for(size_t edgeDownPortIdx = 0; edgeDownPortIdx < downPortAmount; ++edgeDownPortIdx) {
                const auto compNodeIdx = firstCompNodeIdx + (downPortIdx * downPortAmount) + edgeDownPortIdx;
                m_downPortTable.insert({compNodeIdx, getDownPort(downPortIdx)});

                spdlog::trace("Aggregate Switch({}): Mapped computing node #{} with down port #{}.", m_ID, compNodeIdx, downPortIdx);
            }
        }
    }
}

bool Aggregate::tick()
{
    // Advance all ports
    for(auto &port : m_ports) {
        port.tick();
    }

    // Check all ports for incoming messages
    // TODO Should we process one message for each port at every tick?
    for(size_t portIdx = 0; portIdx < m_ports.size(); ++portIdx) {
        auto &sourcePort = m_ports.at(portIdx);

        if(!sourcePort.hasIncoming()) {
            continue;
        }

        auto msg = sourcePort.popIncoming();
        spdlog::trace("Aggregate Switch({}): Message received from sourcePort #{} destined to computing node #{}.", m_ID, portIdx, msg->m_destinationID);

        // Decide on direction (up or down)
        if(auto search = m_downPortTable.find(msg->m_destinationID); search != m_downPortTable.end()) {
            spdlog::trace("Aggregate Switch({}): Redirecting to a down-port..", m_ID);

            auto &targetPort = search->second;

            targetPort.pushOutgoing(std::move(msg));
        }
        else { // Re-direct to up-sourcePort(s)
            spdlog::trace("Aggregate Switch({}): Redirecting to an up-port..", m_ID);

            auto portSearchPolicy = [&](const Port &port1, const Port &port2) -> bool
            {
                return (port1.outgoingAmount() < port2.outgoingAmount());
            };

            static const std::size_t upPortAmount = m_portAmount / 2;
            auto targetPort = std::min_element(m_ports.begin(), m_ports.begin() + upPortAmount, portSearchPolicy);

            targetPort->pushOutgoing(std::move(msg));
        }
    }

    return true;
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
