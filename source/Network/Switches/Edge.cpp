#include "Edge.hpp"
#include "spdlog/spdlog.h"

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

    // Initialize reduce-all requests
    {
        /* A reduce-all request coming from a down-port will be utilized in in-switch reduce-all operation.
         * When all down-ports have sent their data and their data has been reduced, a reduce-all request to all up-ports will be generated.
         *
         * After sending reduce-all request to all up-ports, the switch will wait for the reduced data from all up-ports.
         * When all up-ports have sent their data, the reduce-all response will be sent to all down-ports.
         * Note that the reduce-all response of all up-ports must be the same.
         *
         * When waiting for the reduced data from all up-ports, the switch cannot receive any reduce request from down-ports.
         */

        // To-up
        {
            // Down-port receive flags
            for(std::size_t portIdx = getUpPortAmount(); portIdx < m_portAmount; ++portIdx) {
                m_reduceAllStates.toUp.receiveFlags.insert({portIdx, false});
            }

            if(m_reduceAllStates.toUp.receiveFlags.size() != getDownPortAmount()) {
                spdlog::critical("Edge Switch({}): Amount of up-port reduce-all requests is not equal to down-port amount!", m_ID);

                throw std::runtime_error("Invalid mapping!");
            }
        }

        // To-down
        {
            // Up-port receive flags
            for(std::size_t portIdx = 0; portIdx < getUpPortAmount(); ++portIdx) {
                m_reduceAllStates.toDown.receiveFlags.insert({portIdx, false});
            }

            if(m_reduceAllStates.toDown.receiveFlags.size() != getUpPortAmount()) {
                spdlog::critical("Edge Switch({}): Amount of down-port reduce-all requests is not equal to up-port amount!", m_ID);

                throw std::runtime_error("Invalid mapping!");
            }
        }
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
        const bool bDownPort = (sourcePortIdx >= getUpPortAmount());
        auto &sourcePort = m_ports.at(sourcePortIdx);

        if(!sourcePort.hasIncoming()) {
            continue;
        }

        auto anyMsg = sourcePort.popIncoming();

        switch(anyMsg->type()) {
            case Messages::e_Type::DirectMessage: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::DirectMessage>(static_cast<Messages::DirectMessage*>(anyMsg.release()))));
                break;
            }
            case Messages::e_Type::Acknowledge: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::Acknowledge>(static_cast<Messages::Acknowledge*>(anyMsg.release()))));
                break;
            }
            case Messages::e_Type::BroadcastMessage: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::BroadcastMessage>(static_cast<Messages::BroadcastMessage*>(anyMsg.release()))));
                break;
            }
            case Messages::e_Type::BarrierRequest: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::BarrierRequest>(static_cast<Messages::BarrierRequest*>(anyMsg.release()))));
                break;
            }
            case Messages::e_Type::BarrierRelease: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::BarrierRelease>(static_cast<Messages::BarrierRelease*>(anyMsg.release()))));
                break;
            }
            case Messages::e_Type::Reduce: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::Reduce>(static_cast<Messages::Reduce*>(anyMsg.release()))));
                break;
            }
            case Messages::e_Type::ReduceAll: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::ReduceAll>(static_cast<Messages::ReduceAll*>(anyMsg.release()))));
                break;
            }
            default: {
                spdlog::error("Edge Switch({}): Cannot determine the type of received message!", m_ID);
                spdlog::debug("Type name was {}", anyMsg->typeToString());

                return false;
            }
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

void Edge::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::DirectMessage> msg)
{
    if(!msg) {
        spdlog::error("Edge Switch({}): Received null direct message!", m_ID);

        throw std::runtime_error("Null direct message!");
    }

    spdlog::trace("Edge Switch({}): Message received from port #{} destined to computing node #{}.", m_ID, sourcePortIdx, msg->m_destinationID);

    // Decide on direction (up or down)
    if(auto search = m_downPortTable.find(msg->m_destinationID); search != m_downPortTable.end()) {
        spdlog::trace("Edge Switch({}): Redirecting to a down-port..", m_ID);

        auto &targetPort = search->second;
        targetPort.pushOutgoing(std::move(msg));
    }
    else { // Re-direct to an up-port
        spdlog::trace("Edge Switch({}): Redirecting to an up-port..", m_ID);

        getAvailableUpPort().pushOutgoing(std::move(msg));
    }
}

void Edge::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Acknowledge> msg)
{
    spdlog::trace("Edge Switch({}): Acknowledge received from port #{} destined to computing node #{}.", m_ID, sourcePortIdx, msg->m_destinationID);

    // Decide on direction (up or down)
    if(auto search = m_downPortTable.find(msg->m_destinationID); search != m_downPortTable.end()) {
        spdlog::trace("Edge Switch({}): Redirecting to a down-port..", m_ID);

        auto &targetPort = search->second;
        targetPort.pushOutgoing(std::move(msg));
    }
    else { // Re-direct to an up-port
        spdlog::trace("Edge Switch({}): Redirecting to an up-port..", m_ID);

        getAvailableUpPort().pushOutgoing(std::move(msg));
    }
}

void Edge::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::BroadcastMessage> msg)
{
    spdlog::trace("Edge Switch({}): Broadcast message received from port #{}", m_ID, sourcePortIdx);

    // Decide on direction
    if(sourcePortIdx >= getUpPortAmount()) { // Coming from a down-port
        spdlog::trace("Edge Switch({}): Redirecting to other down-ports..", m_ID);

        for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
            auto &targetPort = getDownPort(downPortIdx);

            if(m_ports.at(sourcePortIdx) != targetPort) {
                auto uniqueMsg = std::make_unique<Network::Messages::BroadcastMessage>(*msg);
                targetPort.pushOutgoing(std::move(uniqueMsg));
            }
        }

        // Re-direct to up-port with minimum messages in it
        {
            spdlog::trace("Edge Switch({}): Redirecting to an up-port..", m_ID);

            // Avoid copying for the last re-direction as we no more need the message content
            getAvailableUpPort().pushOutgoing(std::move(msg));
        }
    }
    else { // Coming from an up-port
        spdlog::trace("Edge Switch({}): Redirecting to all down-ports..", m_ID);

        for(std::size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
            auto uniqueMsg = std::make_unique<Network::Messages::BroadcastMessage>(*msg);
            getDownPort(downPortIdx).pushOutgoing(std::move(uniqueMsg));
        }
    }
}

void Edge::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::BarrierRequest> msg)
{
    if(sourcePortIdx < getUpPortAmount()) { // Coming from an up-port
        spdlog::critical("Edge Switch({}): Barrier request received from an up-port!", m_ID);
        spdlog::debug("Edge Switch({}): Source ID was #{}!", m_ID, msg->m_sourceID);

        throw std::runtime_error("Barrier request in wrong direction!");
    }

    // Re-direct to all up-ports
    for(std::size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
        auto uniqueMsg = std::make_unique<Network::Messages::BarrierRequest>(*msg);
        getUpPort(upPortIdx).pushOutgoing(std::move(uniqueMsg));
    }
}

void Edge::process(const std::size_t sourcePortIdx, [[maybe_unused]] std::unique_ptr<Messages::BarrierRelease> msg)
{
    if(sourcePortIdx >= getUpPortAmount()) { // Coming from a down-port
        spdlog::critical("Aggregate Switch({}): Barrier release received from a down-port({})!", m_ID, sourcePortIdx - getUpPortAmount());

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
                getDownPort(downPortIdx).pushOutgoing(std::move(std::make_unique<Network::Messages::BarrierRelease>()));
            }

            // Reset all flags
            for(auto &entry : m_barrierReleaseFlags) {
                entry.second = false;
            }
        }
    }
}

void Edge::process(const std::size_t sourcePortIdx, [[maybe_unused]] std::unique_ptr<Messages::Reduce> msg)
{
    // Decide on direction
    const bool bToUp = (m_downPortTable.find(msg->m_destinationID) == m_downPortTable.end());

    if(bToUp) {
        if(sourcePortIdx < getUpPortAmount()) { // Coming from an up-port
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
            state.destinationID = msg->m_destinationID;
            state.opType        = msg->m_opType;
            state.value         = std::move(msg->m_data);

            state.receiveFlags.at(sourcePortIdx) = true;
        }
        else {
            if(state.receiveFlags.at(sourcePortIdx)) {
                spdlog::critical("Edge Switch({}): This port({}) has already sent a reduce message!", m_ID, sourcePortIdx);

                throw std::runtime_error("Edge Switch: This port has already sent a reduce message!");
            }

            if(state.destinationID != msg->m_destinationID) {
                spdlog::critical("Edge Switch({}): In reduce message, the destination ID({}) is different(expected {})!", m_ID, msg->m_destinationID, state.destinationID);

                throw std::runtime_error("Edge Switch: The destination ID is different!");
            }

            if(state.opType != msg->m_opType) {
                spdlog::critical("Edge Switch({}): Wrong reduce operation type from port #{}! Expected {} but received {}", m_ID, sourcePortIdx, Messages::toString(state.opType), Messages::toString(msg->m_opType));

                throw std::runtime_error("Edge Switch: The operation type is different!");
            }

            if(state.value.size() != msg->m_data.size()) {
                spdlog::critical("Edge Switch({}): Received data size({}) is different from the expected({})!", m_ID, msg->m_data.size(), state.value.size());

                throw std::runtime_error("Edge Switch: Received data size is different from the expected!");
            }

            // Apply reduce
            state.receiveFlags.at(sourcePortIdx) = true;

            std::transform(state.value.cbegin(),
                            state.value.cend(),
                            msg->m_data.cbegin(),
                            state.value.begin(),
                            [&state](const auto& lhs, const auto& rhs) { return Messages::reduce(lhs, rhs, state.opType); });

            // Check if all down-ports have sent message
            if(std::all_of(state.receiveFlags.cbegin(), state.receiveFlags.cend(), [](const auto& entry) { return entry.second; })) {
                // Send reduced message to the same column up-port
                auto txMsg = std::make_unique<Messages::Reduce>(state.destinationID, state.opType);
                txMsg->m_data = std::move(state.value);
                state.value.clear();

                getPort(m_reduceStates.sameColumnPortID).pushOutgoing(std::move(txMsg));

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
            state.destinationID = msg->m_destinationID;
            state.opType        = msg->m_opType;
            state.value         = std::move(msg->m_data);
            state.receiveFlags.at(sourcePortIdx) = true;

            if(getPort(sourcePortIdx) == m_downPortTable.at(msg->m_destinationID)) {
                spdlog::critical("Edge Switch({}): Received a reduce message destined to the source itself(Port: #{})!", m_ID, sourcePortIdx);

                throw std::runtime_error("Edge Switch: Received a reduce message destined to the source itself!");
            }
        }
        else {
            if(state.receiveFlags.at(sourcePortIdx)) {
                spdlog::critical("Edge Switch({}): This port({}) has already sent a reduce message!", m_ID, sourcePortIdx);

                throw std::runtime_error("Edge Switch: This port has already sent a reduce message!");
            }

            if(state.destinationID != msg->m_destinationID) {
                spdlog::critical("Edge Switch({}): In reduce message, the destination ID({}) is different(expected {})!", m_ID, msg->m_destinationID, state.destinationID);

                throw std::runtime_error("Edge Switch: The destination ID is different!");
            }

            if(state.opType != msg->m_opType) {
                spdlog::critical("Edge Switch({}): Wrong reduce operation type from port #{}! Expected {} but received {}", m_ID, sourcePortIdx, Messages::toString(state.opType), Messages::toString(msg->m_opType));

                throw std::runtime_error("Edge Switch: The operation type is different!");
            }

            if(state.value.size() != msg->m_data.size()) {
                spdlog::critical("Edge Switch({}): Received data size({}) is different from the expected({})!", m_ID, msg->m_data.size(), state.value.size());

                throw std::runtime_error("Edge Switch: Received data size is different from the expected!");
            }

            // Apply reduce
            state.receiveFlags.at(sourcePortIdx) = true;
            std::transform(state.value.cbegin(),
                            state.value.cend(),
                            msg->m_data.cbegin(),
                            state.value.begin(),
                            [&state](const auto& lhs, const auto& rhs) { return Messages::reduce(lhs, rhs, state.opType); });

            // Check if all up-ports and all other down-ports have sent message
            const auto rxCount = std::count_if(state.receiveFlags.cbegin(), state.receiveFlags.cend(), [](const auto& entry) { return entry.second; });

            if((state.receiveFlags.size() - 1) == rxCount) {
                auto txMsg = std::make_unique<Messages::Reduce>(state.destinationID, state.opType);
                txMsg->m_data = state.value;

                m_downPortTable.at(state.destinationID).pushOutgoing(std::move(txMsg));

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

void Edge::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::ReduceAll> msg)
{
    if(sourcePortIdx >= getUpPortAmount()) { // Coming from a down-port
        auto &state = m_reduceAllStates.toUp;

        // Check if to-down has an ongoing reduce-all operation
        if(m_reduceAllStates.toDown.bOngoing) {
            spdlog::critical("Edge Switch({}): Ongoing reduce-all operation to down-ports!", m_ID);

            throw std::runtime_error("Edge Switch: Ongoing reduce-all operation to down-ports!");
        }

        if(state.bOngoing) {
            // Check if the source port has already sent a reduce-all message
            if(state.receiveFlags.at(sourcePortIdx)) {
                spdlog::critical("Edge Switch({}): This port({}) has already sent a reduce-all message!", m_ID, sourcePortIdx);

                throw std::runtime_error("Edge Switch: This port has already sent a reduce-all message!");
            }

            // Check if the operation type is the same
            if(state.opType != msg->m_opType) {
                spdlog::critical("Edge Switch({}): Wrong reduce-all operation type from port #{}! Expected {} but received {}", m_ID, sourcePortIdx, Messages::toString(state.opType), Messages::toString(msg->m_opType));

                throw std::runtime_error("Edge Switch: The operation type is different!");
            }

            state.receiveFlags.at(sourcePortIdx) = true;
            std::transform(state.value.cbegin(),
                            state.value.cend(),
                            msg->m_data.cbegin(),
                            state.value.begin(),
                            [opType = state.opType](const auto& lhs, const auto& rhs) { return Messages::reduce(lhs, rhs, opType); });

            // Check if all down-ports have sent message
            if(std::all_of(state.receiveFlags.cbegin(), state.receiveFlags.cend(), [](const auto& entry) { return entry.second; })) {
                // Send reduced message to all up-ports
                for(std::size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
                    auto txMsg = std::make_unique<Messages::ReduceAll>(state.opType);
                    txMsg->m_data = state.value;

                    getUpPort(upPortIdx).pushOutgoing(std::move(txMsg));
                }
                state.value.clear();

                // Reset to-up state
                state.bOngoing = false;
                std::transform(state.receiveFlags.begin(),
                                state.receiveFlags.end(),
                                std::inserter(state.receiveFlags, state.receiveFlags.begin()),
                                [](auto& entry) { entry.second = false; return entry; });

                // Set to-down state
                m_reduceAllStates.toDown.bOngoing = true;
            }
        }
        else {
            if(!state.value.empty()) {
                spdlog::critical("Edge Switch({}): Reduce-all to-up value wasn't empty!!", m_ID);

                throw std::runtime_error("Edge Switch: Reduce-all to-up value wasn't empty!!");
            }

            state.bOngoing = true;
            state.opType   = msg->m_opType;
            state.value    = std::move(msg->m_data);
            state.receiveFlags.at(sourcePortIdx) = true;
        }
    }
    else {
        auto &state = m_reduceAllStates.toDown;

        // Check if to-up has an ongoing reduce-all operation
        if(m_reduceAllStates.toUp.bOngoing) {
            spdlog::critical("Edge Switch({}): Ongoing reduce-all operation to up-ports!", m_ID);

            throw std::runtime_error("Edge Switch: Ongoing reduce-all operation to up-ports!");
        }

        if(!state.bOngoing) {
            spdlog::critical("Edge Switch({}): Reduce-all to-down wasn't initiated!", m_ID);

            throw std::runtime_error("Edge Switch: Reduce-all to-down wasn't initiated!");
        }

        // Check if the source port has already sent a reduce-all message
        if(state.receiveFlags.at(sourcePortIdx)) {
            spdlog::critical("Edge Switch({}): This port({}) has already sent a reduce-all message!", m_ID, sourcePortIdx);

            throw std::runtime_error("Edge Switch: This port has already sent a reduce-all message!");
        }

        // Check if this is the first reduce-all message
        if(std::all_of(state.receiveFlags.cbegin(), state.receiveFlags.cend(), [](const auto& entry) { return !entry.second; })) {
            if(!state.value.empty()) {
                spdlog::critical("Edge Switch({}): Reduce-all to-down value wasn't empty!!", m_ID);

                throw std::runtime_error("Edge Switch: Reduce-all to-down value wasn't empty!!");
            }

            state.opType = msg->m_opType;
            state.value  = std::move(msg->m_data);
            state.receiveFlags.at(sourcePortIdx) = true;
        }
        else {
            // Check if the operation type is the same
            if(state.opType != msg->m_opType) {
                spdlog::critical("Edge Switch({}): In reduce-all message, the operation type is different!", m_ID);

                throw std::runtime_error("Edge Switch: The operation type is different!");
            }

            // Check if the data is the same
            if(state.value != msg->m_data) {
                spdlog::critical("Edge Switch({}): In reduce-all message, the data is different!", m_ID);

                throw std::runtime_error("Edge Switch: The data is different!");
            }

            state.receiveFlags.at(sourcePortIdx) = true;

            // Check if all up-ports have sent message
            if(std::all_of(state.receiveFlags.cbegin(), state.receiveFlags.cend(), [](const auto& entry) { return entry.second; })) {
                // Send reduced message to all down-ports
                for(std::size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
                    auto txMsg = std::make_unique<Messages::ReduceAll>(state.opType);
                    txMsg->m_data = state.value;

                    getDownPort(downPortIdx).pushOutgoing(std::move(txMsg));
                }

                // Reset to-down state
                state.bOngoing = false;
                std::transform(state.receiveFlags.begin(),
                                state.receiveFlags.end(),
                                std::inserter(state.receiveFlags, state.receiveFlags.begin()),
                                [](auto& entry) { entry.second = false; return entry; });

                state.value.clear();
            }
        }
    }
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
