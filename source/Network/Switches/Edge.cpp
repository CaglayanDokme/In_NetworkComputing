#include <numeric>
#include "Edge.hpp"
#include "spdlog/spdlog.h"
#include "Network/Derivations.hpp"
#include "InterSwitchMessages.hpp"

using namespace Network::Switches;

Edge::Edge(const std::size_t portAmount)
: ISwitch(nextID++, portAmount), firstCompNodeIdx(m_ID * getDownPortAmount())
{
    spdlog::trace("Created edge switch with ID #{}", m_ID);

    // Calculate look-up table for re-direction to down ports
    {
        for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
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
            for(std::size_t portIdx = 0; portIdx < m_portAmount; ++portIdx) {
                m_reduceAllStates.toDown.receiveFlags.insert({portIdx, false});
            }

            if(m_reduceAllStates.toDown.receiveFlags.size() != m_portAmount) {
                spdlog::critical("Edge Switch({}): Amount of to-down reduce-all requests is not equal to up-port amount!", m_ID);

                throw std::runtime_error("Invalid mapping!");
            }
        }
    }

    // Initialize gather requests
    {
        // To-up
        {
            m_gatherStates.toUp.value.resize(getDownPortAmount());
            for(std::size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
                m_gatherStates.toUp.value.at(downPortIdx).first = firstCompNodeIdx + downPortIdx;
            }
        }

        // To-down
        {
            m_gatherStates.toDown.value.resize(Utilities::deriveComputingNodeAmount());
        }

        if(m_gatherStates.toUp.value.size() != getDownPortAmount()) {
            spdlog::critical("Edge Switch({}): Amount of to-up gather requests is not equal to down-port amount!", m_ID);

            throw std::runtime_error("Invalid mapping!");
        }

        if(m_gatherStates.toDown.value.size() != Utilities::deriveComputingNodeAmount()) {
            spdlog::critical("Edge Switch({}): Amount of to-down gather requests is not equal to computing node amount!", m_ID);

            throw std::runtime_error("Invalid mapping!");
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
            case Messages::e_Type::Scatter: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::Scatter>(static_cast<Messages::Scatter*>(anyMsg.release()))));
                break;
            }
            case Messages::e_Type::Gather: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::Gather>(static_cast<Messages::Gather*>(anyMsg.release()))));
                break;
            }
            case Messages::e_Type::IS_Scatter: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::InterSwitch::Scatter>(static_cast<Messages::InterSwitch::Scatter*>(anyMsg.release()))));
                break;
            }
            case Messages::e_Type::IS_Gather: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::InterSwitch::Gather>(static_cast<Messages::InterSwitch::Gather*>(anyMsg.release()))));
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
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

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
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

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
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

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
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

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
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

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
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

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
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

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

void Edge::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Scatter> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    spdlog::trace("Edge Switch({}): Scatter message received from port #{}", m_ID, sourcePortIdx);

    if(msg->m_data.empty()) {
        spdlog::critical("Edge Switch({}): Received an empty scatter message from source port #{}!", m_ID, sourcePortIdx);

        throw std::runtime_error("Edge Switch: Received an empty scatter message!");
    }

    if(sourcePortIdx < getUpPortAmount()) { // Coming from an up-port
        spdlog::critical("Edge Switch({}): Scatter message received from an up-port!", m_ID);

        throw std::runtime_error("Edge Switch: Scatter message received from an up-port!");
    }

    static const auto compNodeAmount = Network::Utilities::deriveComputingNodeAmount(m_portAmount);

    if(m_downPortTable.at(msg->m_sourceID) != getPort(sourcePortIdx)) {
        spdlog::critical("Edge Switch({}): Source ID({}) and source port index({}) didn't match in scatter message!", m_ID, msg->m_sourceID, sourcePortIdx);

        throw std::runtime_error("Edge Switch: Source ID and source port index didn't match in scatter message!");
    }

    auto remainingCompNodeAmount = compNodeAmount - 1;

    if(0 != (msg->m_data.size() % remainingCompNodeAmount)) {
        spdlog::critical("Edge Switch({}): Scatter message size({}) is not divisible by remaining computing node amount({})!", m_ID, msg->m_data.size(), compNodeAmount - 1);

        throw std::runtime_error("Edge Switch: Scatter message size is not divisible by computing node amount!");
    }

    const auto chunkSize = msg->m_data.size() / remainingCompNodeAmount;

    // Scatter to other down-ports
    {
        const auto firstDataIdx = firstCompNodeIdx * chunkSize;

        for(std::size_t downPortIdx = 0, dataIdx = firstDataIdx; downPortIdx < getDownPortAmount(); ++downPortIdx) {
            if(getDownPort(downPortIdx) == getPort(sourcePortIdx)) {
                continue;
            }

            spdlog::trace("Edge Switch({}): Redirecting section [{}, {}) to down-port #{}..", m_ID, dataIdx, dataIdx + chunkSize, downPortIdx);

            auto uniqueMsg = std::make_unique<Network::Messages::Scatter>(msg->m_sourceID);
            uniqueMsg->m_data.reserve(chunkSize);
            uniqueMsg->m_data.assign(msg->m_data.cbegin() + dataIdx, msg->m_data.cbegin() + dataIdx + chunkSize);

            getDownPort(downPortIdx).pushOutgoing(std::move(uniqueMsg));

            dataIdx += chunkSize;
        }

        // Remove scattered section
        {
            const std::size_t scatterSize = chunkSize * (getDownPortAmount() - 1);

            spdlog::trace("Edge Switch({}): Removing section [{}, {})..", m_ID, firstDataIdx, firstDataIdx + scatterSize);
            msg->m_data.erase(msg->m_data.cbegin() + firstDataIdx, msg->m_data.cbegin() + firstDataIdx + scatterSize);
        }
    }

    remainingCompNodeAmount = compNodeAmount - getDownPortAmount();

    // Re-direct the rest to aggregate switch
    {
        auto txMsg = std::make_unique<Messages::InterSwitch::Scatter>(msg->m_sourceID);

        txMsg->m_data.resize(remainingCompNodeAmount);

        for(std::size_t i = 0; i < remainingCompNodeAmount; ++i) {
            auto &targetData = txMsg->m_data.at(i).second;
            txMsg->m_data.at(i).first = (i < firstCompNodeIdx) ? i : (i + getDownPortAmount());

            targetData.resize(chunkSize);
            std::copy(msg->m_data.cbegin() + (i * chunkSize), msg->m_data.cbegin() + ((i + 1) * chunkSize), targetData.begin());
        }

        getAvailableUpPort().pushOutgoing(std::move(txMsg));
    }
}

void Edge::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Gather> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    if(msg->m_data.empty()) {
        spdlog::critical("Edge({}): Gather message cannot be empty!", m_ID);

        throw std::invalid_argument("Edge: Gather message cannot be empty!");
    }

    if(sourcePortIdx < getUpPortAmount()) {
        spdlog::critical("Edge({}): Gather message received from up-port #{}!", m_ID, sourcePortIdx);

        throw std::runtime_error("Edge: Gather message received from an up-port!");
    }

    const bool bToUp = (m_downPortTable.find(msg->m_destinationID) == m_downPortTable.end());

    if(bToUp) {
        auto &state = m_gatherStates.toUp;

        if(m_gatherStates.toDown.bOngoing) {
            spdlog::critical("Edge({}): Ongoing transfer to-down!", m_ID);

            throw std::runtime_error("Edge: Ongoing transfer to-down!");
        }

        if(state.bOngoing) {
            if(state.value.size() != getDownPortAmount()) {
                spdlog::critical("Edge({}): Gather state value size is corrupted! Expected size {}, detected {}", m_ID, getDownPortAmount(), state.value.size());

                throw std::runtime_error("Edge: Gather state value size is corrupted!");
            }

            if(state.destinationID != msg->m_destinationID) {
                spdlog::critical("Edge({}): Destination IDs mismatch in gather messages! Expected {}, received {}", m_ID, state.destinationID, msg->m_destinationID);

                throw std::runtime_error("Edge: Destination IDs mismatch in gather messages!");
            }

            if(state.refSize != msg->m_data.size()) {
                spdlog::critical("Edge({}): Data size mismatch in gather messages! Expected {}, received {}", m_ID, state.refSize, msg->m_data.size());

                throw std::runtime_error("Edge: Data size mismatch in gather messages!");
            }

            const auto sourceID = state.value.at(sourcePortIdx - getUpPortAmount()).first;
            if(m_downPortTable.at(sourceID) != getPort(sourcePortIdx)) {
                spdlog::critical("Edge({}): Source ID({}) and source port index({}) didn't match in gather message!", m_ID, sourceID, sourcePortIdx);

                throw std::runtime_error("Edge: Source ID and source port index didn't match in gather message!");
            }

            auto &data = state.value.at(sourcePortIdx - getUpPortAmount()).second;

            if(!data.empty()) {
                spdlog::critical("Edge({}): This port({}) has already sent a gather message!", m_ID, sourcePortIdx);

                throw std::runtime_error("Edge: This port has already sent a gather message!");
            }

            data = std::move(msg->m_data);

            // Re-direct
            if(std::all_of(state.value.cbegin(), state.value.cend(), [](const auto &entry) { return !entry.second.empty(); })) {
                auto txMsg = std::make_unique<Messages::InterSwitch::Gather>(state.destinationID);

                for(auto &pair : state.value) {
                    txMsg->m_data.push_back({pair.first, std::move(pair.second)});
                }

                getAvailableUpPort().pushOutgoing(std::move(txMsg));

                // Reset state
                state.bOngoing = false;
                state.destinationID = Utilities::deriveComputingNodeAmount();
            }
        }
        else {
            state.bOngoing = true;
            state.destinationID = msg->m_destinationID;

            if(state.value.size() != getDownPortAmount()) {
                spdlog::critical("Edge({}): Gather state value size is corrupted! Expected size {}, detected {}", m_ID, getDownPortAmount(), state.value.size());

                throw std::runtime_error("Edge: Gather state value size is corrupted!");
            }

            for(auto &entry : state.value) {
                entry.second.clear();
            }

            state.value.at(sourcePortIdx - getUpPortAmount()).second = std::move(msg->m_data);
            state.refSize = state.value.at(sourcePortIdx - getUpPortAmount()).second.size();
        }
    }
    else {
        auto &state = m_gatherStates.toDown;

        if(state.push(firstCompNodeIdx + (sourcePortIdx - getUpPortAmount()), msg->m_destinationID, std::move(msg->m_data))) {
            auto txMsg = std::make_unique<Messages::Gather>(state.destinationID);

            txMsg->m_data.reserve(std::accumulate(state.value.cbegin(), state.value.cend(), 0, [](int sum, const auto &elem) { return sum + elem.size(); }));

            for(auto &data : state.value) {
                if(data.empty()) {
                    continue;
                }

                txMsg->m_data.insert(txMsg->m_data.end(), data.cbegin(), data.cend());
            }

            m_downPortTable.at(state.destinationID).pushOutgoing(std::move(txMsg));

            // Reset state
            state.reset();
        }
    }
}

void Edge::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Scatter> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    if(msg->m_data.empty()) {
        spdlog::critical("Edge({}): Scatter message cannot be empty!", m_ID);

        throw std::invalid_argument("Edge: Scatter message cannot be empty!");
    }

    if(sourcePortIdx >= getUpPortAmount()) {
        spdlog::critical("Edge({}): Scatter message received from down-port #{}!", m_ID, sourcePortIdx - getUpPortAmount());

        throw std::runtime_error("Edge: Scatter message received from an down-port!");
    }

    if(msg->m_data.size() != getDownPortAmount()) {
        spdlog::critical("Edge({}): Scatter message size({}) is different from the expected({})!", m_ID, msg->m_data.size(), getDownPortAmount());

        throw std::runtime_error("Edge: Scatter message size is different from the expected!");
    }

    // Redirect to computing nodes
    const auto refSize = msg->m_data.at(0).second.size();
    for(size_t compNodeIdx = firstCompNodeIdx; compNodeIdx < (firstCompNodeIdx + getDownPortAmount()); ++compNodeIdx) {
        const auto count = std::count_if(msg->m_data.cbegin(), msg->m_data.cend(), [compNodeIdx](const auto &entry) { return entry.first == compNodeIdx; });

        if(0 == count) {
            spdlog::critical("Edge({}): Scatter message doesn't contain computing node #{}!", m_ID, compNodeIdx);

            throw std::runtime_error("Edge: Scatter message doesn't contain computing node!");
        }
        else if(1 == count) {
            auto txMsg = std::make_unique<Messages::Scatter>(msg->m_sourceID);
            txMsg->m_data = std::move(std::find_if(msg->m_data.cbegin(), msg->m_data.cend(), [compNodeIdx](const auto &entry) { return entry.first == compNodeIdx; })->second);

            if(txMsg->m_data.size() != refSize) {
                spdlog::critical("Edge({}): Scatter message size({}) for computing node #{} is different from the expected({})!", m_ID, txMsg->m_data.size(), compNodeIdx, refSize);

                throw std::runtime_error("Edge: Scatter message size is different from the expected!");
            }

            m_downPortTable.at(compNodeIdx).pushOutgoing(std::move(txMsg));
        }
        else {
            spdlog::critical("Edge({}): Scatter message contains computing node #{} more than once({})!", m_ID, compNodeIdx, count);

            throw std::runtime_error("Edge: Scatter message contains a computing node more than once!");
        }
    }
}

void Edge::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Gather> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    if(msg->m_data.empty()) {
        spdlog::critical("Edge({}): Gather message cannot be empty!", m_ID);

        throw std::invalid_argument("Edge: Gather message cannot be empty!");
    }

    if(sourcePortIdx >= getUpPortAmount()) {
        spdlog::critical("Edge({}): Gather message received from down-port #{}!", m_ID, sourcePortIdx - getUpPortAmount());

        throw std::runtime_error("Edge: Gather message received from an down-port!");
    }

    bool bRedirect = false;
    for(auto &pair : msg->m_data) {
        if(bRedirect) {
            spdlog::critical("Edge({}): Redirect flag asserted too early! Current source ID #{}", m_ID, pair.first);

            throw std::runtime_error("Edge: Redirect flag asserted too early!");
        }

        bRedirect |= m_gatherStates.toDown.push(pair.first, msg->m_destinationID, std::move(pair.second));
    }

    if(bRedirect) {
        auto &state = m_gatherStates.toDown;

        auto txMsg = std::make_unique<Messages::Gather>(state.destinationID);

        txMsg->m_data.reserve(std::accumulate(state.value.cbegin(), state.value.cend(), 0, [](int sum, const auto &elem) { return sum + elem.size(); }));

        for(auto &data : state.value) {
            if(data.empty()) {
                continue;
            }

            txMsg->m_data.insert(txMsg->m_data.end(), data.cbegin(), data.cend());
        }

        m_downPortTable.at(state.destinationID).pushOutgoing(std::move(txMsg));

        // Reset state
        state.reset();
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

bool Edge::GatherState::ToDown::push(const std::size_t compNodeIdx, const std::size_t destID, decltype(Messages::Gather::m_data) &&data)
{
    if(value.size() != Utilities::deriveComputingNodeAmount()) {
        spdlog::critical("Edge: Gather state value size is corrupted! Expected size {}, detected {}", Utilities::deriveComputingNodeAmount(), value.size());

        throw std::runtime_error("Edge: Gather state value size is corrupted!");
    }

    if(data.empty()) {
        spdlog::critical("Edge: Gather message cannot be empty! Source node #{}", compNodeIdx);

        throw std::invalid_argument("Edge: Gather message cannot be empty!");
    }

    if(bOngoing) {
        if(destinationID != destID) {
            spdlog::critical("Edge: Destination IDs mismatch in gather messages! Expected {}, received {}", destinationID, destID);

            throw std::runtime_error("Edge: Destination IDs mismatch in gather messages!");
        }

        if(refSize != data.size()) {
            spdlog::critical("Edge: Data size mismatch in gather messages! Expected {}, received {}", refSize, data.size());

            throw std::runtime_error("Edge: Data size mismatch in gather messages!");
        }

        if(compNodeIdx == destID) {
            spdlog::critical("Edge: Source ID({}) and destination ID({}) are the same in gather message!", compNodeIdx, destID);

            throw std::runtime_error("Edge: Source ID and destination ID are the same in gather message!");
        }

        value.at(compNodeIdx) = std::move(data);

        // Redirect
        const auto rxCount = std::count_if(value.cbegin(), value.cend(), [](const auto &entry) { return !entry.empty(); });

        return ((value.size() - 1) == rxCount); // Exclude the destination node
    }
    else {
        bOngoing = true;
        destinationID = destID;

        for(auto &entry : value) {
            entry.clear();
        }

        value.at(compNodeIdx) = std::move(data);
        refSize = value.at(compNodeIdx).size();
    }

    return false;
}

void Edge::GatherState::ToDown::reset()
{
    bOngoing = false;
    destinationID = Utilities::deriveComputingNodeAmount();

    for(auto &entry : value) {
        entry.clear();
    }
}
