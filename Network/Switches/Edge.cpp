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

    // Initialize reduce requests
    {
        /* A reduce request destined to an up-port must wait for the data of other down-ports.
         * When all down-ports have sent their data, the reduce request is released.
         * Reduce message can only be sent to the same column aggregate switch
         *
         * A reduce request destined to a down-port must wait for the data of other up-ports and other down-ports(other than the destined port).
         * When all up-ports and other down-ports have sent their data, the reduce request is released.
         *
         * To improve synchronization in system, it's forbidden to have up-port and down-port reduce requests at the same time.
         */

        // To-up
        {
            for(std::size_t portIdx = getUpPortAmount(); portIdx < m_portAmount; ++portIdx) {
                m_reduceStates.toUp.receiveFlags.insert({portIdx, false});
            }

            if(m_reduceStates.toUp.receiveFlags.size() != getDownPortAmount()) {
                spdlog::critical("Edge Switch({}): Amount of up-port reduce requests is not equal to down-port amount!", m_ID);

                throw std::runtime_error("Invalid mapping!");
            }
        }

        // To-down
        {
            for(std::size_t portIdx = 0; portIdx < m_portAmount; ++portIdx) {
                m_reduceStates.toDown.receiveFlags.insert({portIdx, false});
            }

            if(m_reduceStates.toDown.receiveFlags.size() != m_portAmount) {
                spdlog::critical("Edge Switch({}): Amount of down-port reduce requests is not equal to the total port amount!", m_ID);

                throw std::runtime_error("Invalid mapping!");
            }
        }

        const std::size_t aggSwitchPerGroup = getDownPortAmount();
        const std::size_t localColumnIdx    = m_ID % aggSwitchPerGroup; // Column index in the group
        m_reduceStates.sameColumnPortID     = localColumnIdx;
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

        if(typeid(Network::Message) == anyMsg->type()) {
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
        else if(typeid(Network::BroadcastMessage) == anyMsg->type()) {
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
        else if(typeid(Network::BarrierRequest) == anyMsg->type()) {
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
        else if(typeid(Network::BarrierRelease) == anyMsg->type()) {
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
        else if(typeid(Network::Reduce) == anyMsg->type()) {
            const auto &msg = std::any_cast<const Network::Reduce&>(*anyMsg);

            // Decide on direction
            const bool bToUp = (m_downPortTable.find(msg.m_destinationID) == m_downPortTable.end());;

            if(bToUp) {
                if(!downPort) {
                    spdlog::critical("Edge Switch({}): Received a reduce message destined to up and from an up-port!", m_ID);

                    throw std::runtime_error("Edge Switch: Received a reduce message destined to up and from an up-port!");
                }

                auto &state = m_reduceStates.toUp;

                // Check if there was an ongoing transfer to down
                if(m_reduceStates.toDown.bOngoing) {
                    spdlog::critical("Edge Switch({}): Ongoing transfer to down-port!", m_ID);

                    throw std::runtime_error("Edge Switch: Ongoing transfer to down-port!");
                }

                if(!state.bOngoing) {
                    state.bOngoing      = true;
                    state.destinationID = msg.m_destinationID;
                    state.opType        = msg.m_opType;
                    state.value         = msg.m_data;

                    state.receiveFlags.at(sourcePortIdx) = true;
                }
                else {
                    if(state.receiveFlags.at(sourcePortIdx)) {
                        spdlog::critical("Edge Switch({}): This port({}) has already sent a reduce message!", m_ID, sourcePortIdx);

                        throw std::runtime_error("Edge Switch: This port has already sent a reduce message!");
                    }

                    if(state.destinationID != msg.m_destinationID) {
                        spdlog::critical("Edge Switch({}): In reduce message, the destination ID({}) is different(expected {})!", m_ID, msg.m_destinationID, state.destinationID);

                        throw std::runtime_error("Edge Switch: The destination ID is different!");
                    }

                    if(state.opType != msg.m_opType) {
                        spdlog::critical("Edge Switch({}): In reduce message, the operation type is different!", m_ID);

                        throw std::runtime_error("Edge Switch: The operation type is different!");
                    }

                    // Apply reduce
                    state.receiveFlags.at(sourcePortIdx) = true;

                    switch(state.opType) {
                        case Network::Reduce::OpType::Max: {
                            state.value = std::max(state.value, msg.m_data);

                            break;
                        }
                        case Network::Reduce::OpType::Min: {
                            state.value = std::min(state.value, msg.m_data);

                            break;
                        }
                        case Network::Reduce::OpType::Sum: {
                            state.value += msg.m_data;

                            break;
                        }
                        case Network::Reduce::OpType::Multiply: {
                            state.value *= msg.m_data;

                            break;
                        }
                        default: {
                            spdlog::critical("Edge Switch({}): Received reduce message with unknown operation type from port #{}!", m_ID, sourcePortIdx);

                            throw std::runtime_error("Edge Switch: Unknown operation type in reduce messages!");
                        }
                    }

                    // Check if all down-ports have sent message
                    if(std::all_of(state.receiveFlags.cbegin(), state.receiveFlags.cend(), [](const auto& entry) { return entry.second; })) {
                        // Send reduced message to the same column up-port
                        auto txMsg = Network::Reduce(state.destinationID, state.opType);
                        txMsg.m_data = state.value;

                        getPort(m_reduceStates.sameColumnPortID).pushOutgoing(std::make_unique<std::any>(txMsg));

                        // Reset to-up state
                        state.bOngoing = false;
                        std::transform(state.receiveFlags.begin(),
                                       state.receiveFlags.end(),
                                       std::inserter(state.receiveFlags, state.receiveFlags.begin()),
                                       [](auto& entry) { entry.second = false; return entry; });
                    }
                }
            }
            else {
                auto &state = m_reduceStates.toDown;

                // Check if there was an ongoing transfer to up
                if(m_reduceStates.toUp.bOngoing) {
                    spdlog::critical("Edge Switch({}): Ongoing transfer to up-port!", m_ID);

                    throw std::runtime_error("Edge Switch: Ongoing transfer to up-port!");
                }

                if(!state.bOngoing) {
                    state.bOngoing      = true;
                    state.destinationID = msg.m_destinationID;
                    state.opType        = msg.m_opType;
                    state.value         = msg.m_data;
                    state.receiveFlags.at(sourcePortIdx) = true;

                    if(getPort(sourcePortIdx) == m_downPortTable.at(msg.m_destinationID)) {
                        spdlog::critical("Edge Switch({}): Received a reduce message destined to the source itself(Port: #{})!", m_ID, sourcePortIdx);

                        throw std::runtime_error("Edge Switch: Received a reduce message destined to the source itself!");
                    }
                }
                else {
                    if(state.receiveFlags.at(sourcePortIdx)) {
                        spdlog::critical("Edge Switch({}): This port({}) has already sent a reduce message!", m_ID, sourcePortIdx);

                        throw std::runtime_error("Edge Switch: This port has already sent a reduce message!");
                    }

                    if(state.destinationID != msg.m_destinationID) {
                        spdlog::critical("Edge Switch({}): In reduce message, the destination ID({}) is different(expected {})!", m_ID, msg.m_destinationID, state.destinationID);

                        throw std::runtime_error("Edge Switch: The destination ID is different!");
                    }

                    if(state.opType != msg.m_opType) {
                        spdlog::critical("Edge Switch({}): In reduce message, the operation type is different!", m_ID);

                        throw std::runtime_error("Edge Switch: The operation type is different!");
                    }

                    // Apply reduce
                    state.receiveFlags.at(sourcePortIdx) = true;

                    switch(state.opType) {
                        case Network::Reduce::OpType::Max: {
                            state.value = std::max(state.value, msg.m_data);

                            break;
                        }
                        case Network::Reduce::OpType::Min: {
                            state.value = std::min(state.value, msg.m_data);

                            break;
                        }
                        case Network::Reduce::OpType::Sum: {
                            state.value += msg.m_data;

                            break;
                        }
                        case Network::Reduce::OpType::Multiply: {
                            state.value *= msg.m_data;

                            break;
                        }
                        default: {
                            spdlog::critical("Edge Switch({}): Received reduce message with unknown operation type from port #{}!", m_ID, sourcePortIdx);

                            throw std::runtime_error("Edge Switch: Unknown operation type in reduce messages!");
                        }
                    }

                    // Check if all up-ports and all other down-ports have sent message
                    const auto rxCount = std::count_if(state.receiveFlags.cbegin(), state.receiveFlags.cend(), [](const auto& entry) { return entry.second; });

                    if((state.receiveFlags.size() - 1) == rxCount) {
                        auto txMsg = Network::Reduce(state.destinationID, state.opType);
                        txMsg.m_data = state.value;

                        m_downPortTable.at(state.destinationID).pushOutgoing(std::make_unique<std::any>(txMsg));

                        // Reset to-down state
                        state.bOngoing = false;
                        std::transform(state.receiveFlags.begin(),
                                       state.receiveFlags.end(),
                                       std::inserter(state.receiveFlags, state.receiveFlags.begin()),
                                       [](auto& entry) { entry.second = false; return entry; });
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
