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
        const std::size_t downPortAmount        = getDownPortAmount();
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

    // Initialize barrier release flags
    for(std::size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
        m_barrierReleaseFlags.insert({upPortIdx, false});
    }

    // Initialize reduce requests
    {
        /* A reduce request destined to an up-port can be redirected immediately.
         * On the other hand, a reduce request destined to a down-port will be delayed until all up-ports have received
         * the same message. If the destination port placed in the same column, only the up-port messages would be enough.
         * In other case, the reduce message of the edge switch placed on the same column will be waited on.
         */

        for(std::size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx){
            m_reduceStates.flags.insert({upPortIdx, false});
        }

        const std::size_t aggSwitchPerGroup = getDownPortAmount();
        const std::size_t localColumnIdx    = m_ID % aggSwitchPerGroup; // Column index in the group
        m_reduceStates.sameColumnPortID    = getUpPortAmount() + localColumnIdx;

        m_reduceStates.flags.insert({m_reduceStates.sameColumnPortID, false});
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
    for(size_t sourcePortIdx = 0; sourcePortIdx < m_ports.size(); ++sourcePortIdx) {
        const bool bDownPort = (sourcePortIdx >= getUpPortAmount());
        auto &sourcePort = m_ports.at(sourcePortIdx);

        if(!sourcePort.hasIncoming()) {
            continue;
        }

        auto anyMsg = sourcePort.popIncoming();

        if(typeid(Network::Message) == anyMsg->type()) {
            const auto &msg = std::any_cast<const Network::Message&>(*anyMsg);

            spdlog::trace("Aggregate Switch({}): Message received from sourcePort #{} destined to computing node #{}.", m_ID, sourcePortIdx, msg.m_destinationID);

            // Decide on direction (up or down)
            if(auto search = m_downPortTable.find(msg.m_destinationID); search != m_downPortTable.end()) {
                spdlog::trace("Aggregate Switch({}): Redirecting to a down-port..", m_ID);

                auto &targetPort = search->second;

                targetPort.pushOutgoing(std::move(anyMsg));
            }
            else { // Re-direct to up-port(s)
                spdlog::trace("Aggregate Switch({}): Redirecting to an up-port..", m_ID);

                getAvailableUpPort().pushOutgoing(std::move(anyMsg));
            }
        }
        else if(typeid(Network::BroadcastMessage) == anyMsg->type()) {
            spdlog::trace("Aggregate Switch({}): Broadcast message received from port #{}", m_ID, sourcePortIdx);

            // Decide on direction
            if(bDownPort) { // Coming from a down-port
                spdlog::trace("Aggregate Switch({}): Redirecting to other down-ports..", m_ID);

                for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
                    auto &targetPort = getDownPort(downPortIdx);

                    if(sourcePort != targetPort) {
                        targetPort.pushOutgoing(std::make_unique<std::any>(*anyMsg));
                    }
                }

                // Re-direct to up-port with minimum messages in it
                {
                    spdlog::trace("Aggregate Switch({}): Redirecting to an up-port..", m_ID);

                    getAvailableUpPort().pushOutgoing(std::move(anyMsg));
                }
            }
            else { // Coming from an up-port
                spdlog::trace("Aggregate Switch({}): Redirecting to all down-ports..", m_ID);

                // Re-direct to all down-ports
                for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
                    getDownPort(downPortIdx).pushOutgoing(std::make_unique<std::any>(*anyMsg));
                }
            }
        }
        else if(typeid(Network::BarrierRequest) == anyMsg->type()) {
            if(!bDownPort) {
                const auto &msg = std::any_cast<const Network::BarrierRequest&>(*anyMsg);

                spdlog::critical("Aggregate Switch({}): Barrier request received from an up-port!", m_ID);
                spdlog::debug("Aggregate Switch({}): Source ID was #{}!", m_ID, msg.m_sourceID);

                throw std::runtime_error("Barrier request in wrong direction!");
            }

            // Re-direct to all up-ports
            for(std::size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
                getUpPort(upPortIdx).pushOutgoing(std::make_unique<std::any>(*anyMsg));
            }
        }
        else if(typeid(Network::BarrierRelease) == anyMsg->type()) {
            if(bDownPort) {
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
        else if(typeid(Network::Reduce) == anyMsg->type()) {
            const auto &msg = std::any_cast<const Network::Reduce&>(*anyMsg);

            // Check if source port exists in source table for reduction messages
            if(std::find_if(m_reduceStates.flags.cbegin(), m_reduceStates.flags.cend(), [sourcePortIdx](const auto& entry) { return entry.first == sourcePortIdx; }) == m_reduceStates.flags.cend()) {
                spdlog::error("Aggregate Switch({}): Received a reduction message from an invalid source port(#{})!", m_ID, sourcePortIdx);

                throw std::runtime_error("Received a reduction message from an invalid source port!");
            }

            if(bDownPort) {
                // A reduce message can be received from the same column down-port only
                if(sourcePortIdx == m_reduceStates.sameColumnPortID) {
                    spdlog::error("Aggregate Switch({}): Received a reduce message from an invalid down-port(#{})!", m_ID, sourcePortIdx);

                    throw std::runtime_error(" Received a reduce message from an invalid down-port!");
                }
            }

            const bool bOngoingRedirection = std::all_of(m_reduceStates.flags.cbegin(), m_reduceStates.flags.cend(), [](const auto& entry) { return !entry.second; });

            // Decide on direction (up or down)
            auto search = m_downPortTable.find(msg.m_destinationID);

            if((search != m_downPortTable.end())) {
                if(sourcePortIdx == m_reduceStates.sameColumnPortID) {
                    spdlog::error("Aggregate Switch({}): The same column down-port cannot send reduce messages to the down-ports!", m_ID, sourcePortIdx);

                    throw std::runtime_error("The same column down-port cannot send reduce messages to the down-ports!");
                }

                if(bOngoingRedirection) {
                    // Process message
                    {
                        if(m_reduceStates.flags.at(sourcePortIdx)) {
                            spdlog::critical("Aggregate Switch({}): Received multiple reduce messages from port #{}!", m_ID, sourcePortIdx);

                            throw std::runtime_error("Aggregate Switch: Received multiple reduce messages!");
                        }

                        if(msg.m_opType != m_reduceStates.opType) {
                            spdlog::critical("Aggregate Switch({}): Received reduce messages with different operation types from port #{}!", m_ID, sourcePortIdx);

                            throw std::runtime_error("Aggregate Switch: Operation types doesn't match in reduce messages!");
                        }

                        const bool bFirstUpPortData = !bDownPort && !m_reduceStates.bUpPortReferenceValueSet;
                        if(!bDownPort) {
                            if(bFirstUpPortData) {
                                m_reduceStates.upPortReferenceValue = msg.m_data;
                                m_reduceStates.bUpPortReferenceValueSet = true;
                            }
                            else {
                                if(m_reduceStates.upPortReferenceValue != msg.m_data) {
                                    spdlog::critical("Aggregate Switch({}): Up-port(#{}) value({}) doesn't match the reference value({})!", m_ID, sourcePortIdx, msg.m_data, m_reduceStates.upPortReferenceValue);

                                    throw std::runtime_error("Aggregate Switch: Up-port value doesn't match the reference value!");
                                }
                            }
                        }

                        m_reduceStates.flags.at(sourcePortIdx) = true;

                        if(bDownPort || bFirstUpPortData) {
                            switch(m_reduceStates.opType) {
                                case Network::Reduce::OpType::Max: {
                                    m_reduceStates.value = std::max(m_reduceStates.value, msg.m_data);

                                    break;
                                }
                                case Network::Reduce::OpType::Min: {
                                    m_reduceStates.value = std::min(m_reduceStates.value, msg.m_data);

                                    break;
                                }
                                case Network::Reduce::OpType::Sum: {
                                    m_reduceStates.value += msg.m_data;

                                    break;
                                }
                                case Network::Reduce::OpType::Multiply: {
                                    m_reduceStates.value *= msg.m_data;

                                    break;
                                }
                                default: {
                                    spdlog::critical("Aggregate Switch({}): Received reduce messages with unknown operation type from port #{}!", m_ID, sourcePortIdx);

                                    throw std::runtime_error("Aggregate Switch: Unknown operation type in reduce messages!");
                                }
                            }
                        }
                    }

                    // Check if all reduce messages are received
                    {
                        const auto rxCount = std::count_if(m_reduceStates.flags.cbegin(), m_reduceStates.flags.cend(),
                                                           [](const auto &entry)
                                                           { return entry.second; });

                        auto &destinationPort = m_downPortTable.at(m_reduceStates.destinationID);
                        const bool bDestinedToSameColumn = (destinationPort == getPort(m_reduceStates.sameColumnPortID));

                        if(bDestinedToSameColumn) {
                            if(m_reduceStates.flags.at(m_reduceStates.sameColumnPortID)) {
                                spdlog::critical("Aggregate Switch({}): Although destined to the same column port, received reduce message from that port!", m_ID);

                                throw std::runtime_error("Aggregate Switch: Received reduce message from the same column port which is the destined port!");
                            }

                            if(getUpPortAmount() == rxCount) {
                                spdlog::trace("Aggregate Switch({}): All reduce messages received from all up-ports! Re-directing..", m_ID);

                                auto msg = Network::Reduce(m_reduceStates.destinationID, m_reduceStates.opType);
                                msg.m_data = m_reduceStates.value;

                                destinationPort.pushOutgoing(std::make_unique<std::any>(msg));

                                std::transform(m_reduceStates.flags.begin(),
                                               m_reduceStates.flags.end(),
                                               std::inserter(m_reduceStates.flags, m_reduceStates.flags.begin()),
                                               [](auto& entry) { entry.second = false; return entry; });
                                m_reduceStates.bUpPortReferenceValueSet = false;
                            }
                        }
                        else {
                            if(m_reduceStates.flags.size() == rxCount) { // i.e. getUpPortAmount() + 1
                                spdlog::trace("Aggregate Switch({}): All reduce messages received from all up-ports and the same column down-port! Re-directing..", m_ID);

                                auto msg = Network::Reduce(m_reduceStates.destinationID, m_reduceStates.opType);
                                msg.m_data = m_reduceStates.value;

                                destinationPort.pushOutgoing(std::make_unique<std::any>(msg));

                                std::transform(m_reduceStates.flags.begin(),
                                               m_reduceStates.flags.end(),
                                               std::inserter(m_reduceStates.flags, m_reduceStates.flags.begin()),
                                               [](auto& entry) { entry.second = false; return entry; });
                                m_reduceStates.bUpPortReferenceValueSet = false;
                            }
                        }
                    }
                }
                else {
                    m_reduceStates.destinationID = msg.m_destinationID;
                    m_reduceStates.opType        = msg.m_opType;
                    m_reduceStates.value         = msg.m_data;

                    if(!bDownPort) {
                        m_reduceStates.upPortReferenceValue = msg.m_data;
                        m_reduceStates.bUpPortReferenceValueSet = true;
                    }

                    m_reduceStates.flags.at(sourcePortIdx) = true;
                }
            }
            else {
                if(bOngoingRedirection) {
                    spdlog::error("Aggregate Switch({}): Switch was in the middle of a reduction through a down-port!", m_ID, sourcePortIdx);

                    throw std::runtime_error("Switch was in middle of a reduction through a down-port!");
                }

                // Re-direct to all up-ports
                for(std::size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
                    getUpPort(upPortIdx).pushOutgoing(std::make_unique<std::any>(msg));
                }
            }
        }
        else {
            spdlog::error("Aggregate Switch({}): Cannot determine the type of received message!", m_ID);
            spdlog::debug("Type name was {}", anyMsg->type().name());

            return false;
        }
    }

    return true;
}

Network::Port &Aggregate::getUpPort(const size_t &portID)
{
    if(portID >= getUpPortAmount()) {
        spdlog::error("Switch doesn't have an up-port with ID {}", portID);

        throw std::invalid_argument("Invalid up-port ID!");
    }

    return getPort(portID);
}

Network::Port &Aggregate::getDownPort(const size_t &portID)
{
    if(portID >= getDownPortAmount()) {
        spdlog::error("Switch doesn't have a down-port with ID {}", portID);

        throw std::invalid_argument("Invalid down-port ID!");
    }

    return getPort(getUpPortAmount() + portID);
}

Network::Port &Aggregate::getAvailableUpPort()
{
    // Find the up-port with the least messages in it
    auto portSearchPolicy = [&](const Port &port1, const Port &port2) -> bool
    {
        return (port1.outgoingAmount() < port2.outgoingAmount());
    };

    return *std::min_element(m_ports.begin(), m_ports.begin() + getUpPortAmount(), portSearchPolicy);
}

std::size_t Aggregate::getDownPortAmount() const
{
    static const std::size_t downPortAmount = m_portAmount / 2;

    return downPortAmount;
}

std::size_t Aggregate::getUpPortAmount() const
{
    static const std::size_t upPortAmount = m_portAmount / 2;

    return upPortAmount;
}
