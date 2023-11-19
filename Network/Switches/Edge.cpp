#include "Edge.hpp"
#include "spdlog/spdlog.h"
#include "Network/Message.hpp"

using namespace Network::Switches;

Edge::Edge(const std::size_t portAmount)
: ISwitch(nextID++, portAmount)
{
    spdlog::trace("Created edge switch with ID #{}", m_ID);

    // Calculate look-up table for re-direction to down ports
    {
        const std::size_t downPortAmount = m_portAmount / 2;
        const std::size_t firstCompNodeIdx = m_ID * downPortAmount;

        for(size_t downPortIdx = 0; downPortIdx < downPortAmount; ++downPortIdx) {
            const auto compNodeIdx = firstCompNodeIdx + downPortIdx;
            m_downPortTable.insert({compNodeIdx, getDownPort(downPortIdx)});

            spdlog::trace("Edge Switch({}): Mapped computing node #{} with down port #{}.", m_ID, compNodeIdx, downPortIdx);
        }
    }

    // Initialize barrier release flags
    for(std::size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
        m_barrierReleaseFlags.insert({upPortIdx, false});
    }
}

bool Edge::tick()
{
    // Advance all ports
    for(auto &port : m_ports) {
        port.tick();
    }

    // Check all ports for incoming messages
    // TODO Should we process one message for each port at every tick?
    for(size_t sourcePortIdx = 0; sourcePortIdx < m_ports.size(); ++sourcePortIdx) {
        const bool downPort = (sourcePortIdx >= getUpPortAmount());
        auto &sourcePort = m_ports.at(sourcePortIdx);

        if(!sourcePort.hasIncoming()) {
            continue;
        }

        auto anyMsg = sourcePort.popIncoming();

        if(anyMsg->type() == typeid(Network::Message)) {
            const auto &msg = std::any_cast<const Network::Message&>(*anyMsg);

            spdlog::trace("Edge Switch({}): Message received from port #{} destined to computing node #{}.", m_ID, sourcePortIdx, msg.m_destinationID);

            // Decide on direction (up or down)
            if(auto search = m_downPortTable.find(msg.m_destinationID); search != m_downPortTable.end()) {
                spdlog::trace("Edge Switch({}): Redirecting to a down-port..", m_ID);

                auto &targetPort = search->second;
                targetPort.pushOutgoing(std::move(anyMsg));
            } else { // Re-direct to an up-port
                spdlog::trace("Edge Switch({}): Redirecting to an up-port..", m_ID);

                getAvailableUpPort().pushOutgoing(std::move(anyMsg));
            }
        }
        else if(anyMsg->type() == typeid(Network::BroadcastMessage)) {
            spdlog::trace("Edge Switch({}): Broadcast message received from port #{}", m_ID, sourcePortIdx);

            // Decide on direction
            if(downPort) { // Coming from a down-port
                spdlog::trace("Edge Switch({}): Redirecting to other down-ports..", m_ID);

                for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
                    auto &targetPort = getDownPort(downPortIdx);

                    if(sourcePort != targetPort) {
                        targetPort.pushOutgoing(std::make_unique<std::any>(*anyMsg));
                    }
                }

                // Re-direct to up-port with minimum messages in it
                {
                    spdlog::trace("Edge Switch({}): Redirecting to an up-port..", m_ID);

                    getAvailableUpPort().pushOutgoing(std::move(anyMsg));
                }
            }
            else { // Coming from an up-port
                spdlog::trace("Edge Switch({}): Redirecting to all down-ports..", m_ID);

                for(std::size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
                    getDownPort(downPortIdx).pushOutgoing(std::make_unique<std::any>(*anyMsg));
                }
            }
        }
        else if(anyMsg->type() == typeid(Network::BarrierRequest)) {
            if(!downPort) {
                const auto &msg = std::any_cast<const Network::BarrierRequest&>(*anyMsg);

                spdlog::critical("Edge Switch({}): Barrier request received from an up-port!", m_ID);
                spdlog::debug("Edge Switch({}): Source ID was #{}!", m_ID, msg.m_sourceID);

                throw std::runtime_error("Barrier request in wrong direction!");
            }

            // Re-direct to all up-ports
            for(std::size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
                getUpPort(upPortIdx).pushOutgoing(std::make_unique<std::any>(*anyMsg));
            }
        }
        else if(anyMsg->type() == typeid(Network::BarrierRelease)) {
            if(downPort) {
                spdlog::critical("Aggregate Switch({}): Barrier release received from a down-port!", m_ID);

                throw std::runtime_error("Barrier release in wrong direction!");
            }

            // Save into flags
            {
                const auto upPortIdx = sourcePortIdx;
                m_barrierReleaseFlags.at(upPortIdx) = true;
            }

            // Check for barrier release
            {
                if(std::all_of(m_barrierReleaseFlags.begin(), m_barrierReleaseFlags.end(), [](const auto& entry) { return entry.second; })) {
                    // Send barrier release to all down-ports
                    for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
                        getDownPort(downPortIdx).pushOutgoing(std::make_unique<std::any>(Network::BarrierRelease()));
                    }

                    // Reset all flags
                    for(auto &entry : m_barrierReleaseFlags) {
                        entry.second = false;
                    }
                }
            }
        }
        else {
            spdlog::error("Edge Switch({}): Cannot determine the type of received message!", m_ID);
            spdlog::debug("Type name was {}", anyMsg->type().name());

            return false;
        }
    }

    return true;
}

Network::Port &Edge::getUpPort(const size_t &portID)
{
    if(portID >= getUpPortAmount()) {
        spdlog::error("Switch doesn't have an up-port with ID {}", portID);

        throw std::invalid_argument("Invalid up-port ID!");
    }

    return getPort(portID);
}

Network::Port &Edge::getDownPort(const size_t &portID)
{
    if(portID >= getDownPortAmount()) {
        spdlog::error("Switch doesn't have a down-port with ID {}", portID);

        throw std::invalid_argument("Invalid down-port ID!");
    }

    return getPort(getUpPortAmount() + portID);
}

Network::Port &Edge::getAvailableUpPort()
{
    // Find the up-port with the least messages in it
    auto portSearchPolicy = [&](const Port &port1, const Port &port2) -> bool
    {
        return (port1.outgoingAmount() < port2.outgoingAmount());
    };

    return *std::min_element(m_ports.begin(), m_ports.begin() + getUpPortAmount(), portSearchPolicy);
}

std::size_t Edge::getDownPortAmount() const
{
    static const std::size_t downPortAmount = m_portAmount / 2;

    return downPortAmount;
}

std::size_t Edge::getUpPortAmount() const
{
    static const std::size_t upPortAmount = m_portAmount / 2;

    return upPortAmount;
}
