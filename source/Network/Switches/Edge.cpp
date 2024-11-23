#include <numeric>
#include "Edge.hpp"
#include "spdlog/spdlog.h"
#include "Network/Constants.hpp"
#include "InterSwitchMessages.hpp"

using namespace Network::Switches;

Edge::Edge(const size_t portAmount)
: ISwitch(nextID++, portAmount), firstCompNodeIdx(m_ID * getDownPortAmount())
{
    spdlog::trace("Created edge switch with ID #{}", m_ID);

    const size_t subColumnIdx = m_ID % Network::Constants::getSubColumnAmountPerGroup();
    m_sameColumnPortID = subColumnIdx;

    // Calculate look-up table for re-direction to down ports
    {
        for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
            const auto compNodeIdx = firstCompNodeIdx + downPortIdx;
            m_downPortTable.insert({compNodeIdx, getDownPort(downPortIdx)});

            spdlog::trace("Edge Switch({}): Mapped computing node #{} with down port #{}.", m_ID, compNodeIdx, downPortIdx);
        }
    }

    // Initialize barrier flags
    {
        // Request flags
        for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
            m_barrierRequestFlags.insert({downPortIdx, false});
        }

        // Release flags
        for(size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
            m_barrierReleaseFlags.insert({upPortIdx, false});
        }
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
            for(size_t portIdx = getUpPortAmount(); portIdx < m_portAmount; ++portIdx) {
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
            for(size_t portIdx = 0; portIdx < getUpPortAmount(); ++portIdx) {
                m_reduceAllStates.toDown.receiveFlags.insert({portIdx, false});
            }

            if(m_reduceAllStates.toDown.receiveFlags.size() != getUpPortAmount()) {
                spdlog::critical("Edge Switch({}): Amount of to-down reduce-all requests is not equal to up-port amount!", m_ID);

                throw std::runtime_error("Invalid mapping!");
            }
        }
    }

    // Initialize all-gather state(s)
    {
        // To-down
        {
            for(size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
                m_allGatherStates.toDown.receiveFlags.insert({upPortIdx, false});
            }
        }

        if(m_allGatherStates.toDown.receiveFlags.size() != getUpPortAmount()) {
            spdlog::critical("Edge Switch({}): Amount of to-down all-gather requests is not equal to up-port amount!", m_ID);

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
    bool bMsgReceived = false;
    for(size_t checkedPortAmount = 0; (checkedPortAmount < m_ports.size()) && !bMsgReceived; ++checkedPortAmount, m_nextPort = (m_nextPort + 1) % m_portAmount) {
        const auto sourcePortIdx = m_nextPort;
        auto &sourcePort = m_ports.at(sourcePortIdx);

        if(!sourcePort.hasIncoming()) {
            continue;
        }

        auto anyMsg = sourcePort.popIncoming();

        bMsgReceived = true;
        ++m_statistics.totalProcessedMessages;

        spdlog::trace("Edge Switch({}): Received {} from sourcePort #{}.", m_ID, anyMsg->typeToString(), sourcePortIdx);

        // Is network computing enabled?
        if(!canCompute()) {
            redirect(sourcePortIdx, std::move(anyMsg));

            continue;
        }

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
            case Messages::e_Type::AllGather: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::AllGather>(static_cast<Messages::AllGather*>(anyMsg.release()))));
                break;
            }
            case Messages::e_Type::IS_Reduce: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::InterSwitch::Reduce>(static_cast<Messages::InterSwitch::Reduce*>(anyMsg.release()))));
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
            case Messages::e_Type::IS_AllGather: {
                process(sourcePortIdx, std::move(std::unique_ptr<Messages::InterSwitch::AllGather>(static_cast<Messages::InterSwitch::AllGather*>(anyMsg.release()))));
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

void Edge::process(const size_t sourcePortIdx, std::unique_ptr<Messages::DirectMessage> msg)
{
    if(!msg) {
        spdlog::error("Edge Switch({}): Received null direct message!", m_ID);

        throw std::runtime_error("Null direct message!");
    }

    redirect(sourcePortIdx, std::move(msg));
}

void Edge::process(const size_t sourcePortIdx, std::unique_ptr<Messages::Acknowledge> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    spdlog::trace("Edge Switch({}): Acknowledge received from port #{} destined to computing node #{}.", m_ID, sourcePortIdx, msg->m_destinationID.value());

    // Decide on direction (up or down)
    if(auto search = m_downPortTable.find(msg->m_destinationID.value()); search != m_downPortTable.end()) {
        spdlog::trace("Edge Switch({}): Redirecting to a down-port..", m_ID);

        auto &targetPort = search->second;
        targetPort.pushOutgoing(std::move(msg));
    }
    else { // Re-direct to an up-port
        spdlog::trace("Edge Switch({}): Redirecting to an up-port..", m_ID);

        getAvailableUpPort().pushOutgoing(std::move(msg));
    }
}

void Edge::process(const size_t sourcePortIdx, std::unique_ptr<Messages::BroadcastMessage> msg)
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

        for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
            auto uniqueMsg = std::make_unique<Network::Messages::BroadcastMessage>(*msg);
            getDownPort(downPortIdx).pushOutgoing(std::move(uniqueMsg));
        }
    }
}

void Edge::process(const size_t sourcePortIdx, std::unique_ptr<Messages::BarrierRequest> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    if(sourcePortIdx < getUpPortAmount()) { // Coming from an up-port
        spdlog::critical("Edge Switch({}): Barrier request received from an up-port!", m_ID);
        spdlog::debug("Edge Switch({}): Source ID was #{}!", m_ID, msg->m_sourceID.value());

        throw std::runtime_error("Barrier request in wrong direction!");
    }

    // Save into flags
    {
        const auto downPortIdx = sourcePortIdx - getUpPortAmount();

        if(m_barrierRequestFlags.at(downPortIdx)) {
            spdlog::critical("Core Switch({}): Port #{} already sent a barrier request!", m_ID, sourcePortIdx);

            throw std::runtime_error("Core Switch: Port already sent a barrier request!");
        }
        else {
            m_barrierRequestFlags.at(downPortIdx) = true;
        }
    }

    // Check for re-transmission to up-ports
    if(std::all_of(m_barrierRequestFlags.begin(), m_barrierRequestFlags.end(), [](const auto& entry) { return entry.second; })) {
        for(size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
            getUpPort(upPortIdx).pushOutgoing(std::move(std::make_unique<Network::Messages::BarrierRequest>()));
        }

        for(auto &entry : m_barrierRequestFlags) {
            entry.second = false;
        }
    }
}

void Edge::process(const size_t sourcePortIdx, [[maybe_unused]] std::unique_ptr<Messages::BarrierRelease> msg)
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
    if(std::all_of(m_barrierReleaseFlags.begin(), m_barrierReleaseFlags.end(), [](const auto& entry) { return entry.second; })) {
        for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
            getDownPort(downPortIdx).pushOutgoing(std::move(std::make_unique<Network::Messages::BarrierRelease>()));
        }

        for(auto &entry : m_barrierReleaseFlags) {
            entry.second = false;
        }
    }
}

void Edge::process(const size_t sourcePortIdx, [[maybe_unused]] std::unique_ptr<Messages::Reduce> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    if(!msg->m_destinationID.has_value()) {
        spdlog::critical("Edge Switch({}): Reduce message doesn't have a destination ID!", m_ID);

        throw std::runtime_error("Edge Switch: Reduce message doesn't have a destination ID!");
    }

    if(sourcePortIdx < getUpPortAmount()) {
        spdlog::critical("Edge Switch({}): Received a reduce message from an up-port!", m_ID);

        throw std::runtime_error("Edge Switch: Received a reduce message from an up-port!");
    }

    // Decide on direction
    const bool bToUp = !isComputingNodeConnected(msg->m_destinationID.value());

    if(bToUp) {
        if(sourcePortIdx < getUpPortAmount()) { // Coming from an up-port
            spdlog::critical("Edge Switch({}): Received a reduce message destined to up and from an up-port!", m_ID);

            throw std::runtime_error("Edge Switch: Received a reduce message destined to up and from an up-port!");
        }

        auto &state = m_reduceStates.toUp;

        // Check if there was an ongoing transfer to down
        if(m_reduceStates.toDown.has_value()) {
            spdlog::critical("Edge Switch({}): Ongoing reduce operation to down!", m_ID);

            throw std::runtime_error("Edge Switch: Ongoing reduce operation to down!");
        }

        if(!state.has_value()) {
            state.emplace();

            state->m_destinationID = msg->m_destinationID.value();
            state->m_opType        = msg->m_opType;
            state->m_value         = std::move(msg->m_data);
            state->m_contributors.push_back(msg->m_sourceID.value());
        }
        else {
            state->push({msg->m_sourceID.value()}, msg->m_destinationID.value(), msg->m_opType, std::move(msg->m_data));

            // Check if all down-ports have sent message
            if(state->m_contributors.size() == getDownPortAmount()) {
                spdlog::trace("Edge Switch({}): Sending the reduced data to the same column up-port #{}", m_ID, m_sameColumnPortID);

                // Send reduced message to the same column up-port
                auto txMsg = std::make_unique<Messages::InterSwitch::Reduce>(msg->m_destinationID.value());

                txMsg->m_contributors = std::move(state->m_contributors);
                txMsg->m_data         = std::move(state->m_value);
                txMsg->m_opType       = state->m_opType;

                getPort(m_sameColumnPortID).pushOutgoing(std::move(txMsg));

                state.reset();
            }
        }
    }
    else {
        auto &state = m_reduceStates.toDown;

        // Check if there was an ongoing transfer to up
        if(m_reduceStates.toUp.has_value()) {
            spdlog::critical("Edge Switch({}): Ongoing reduce operation to up!", m_ID);

            throw std::runtime_error("Edge Switch: Ongoing reduce operation to up!");
        }

        if(!state.has_value()) {
            state.emplace();

            state->m_destinationID = msg->m_destinationID.value();
            state->m_opType        = msg->m_opType;
            state->m_value         = std::move(msg->m_data);
            state->m_contributors.push_back(msg->m_sourceID.value());
        }
        else {
            state->push({msg->m_sourceID.value()}, msg->m_destinationID.value(), msg->m_opType, std::move(msg->m_data));

            if(state->m_contributors.size() == (Network::Constants::deriveComputingNodeAmount() - 1)) {
                auto txMsg = std::make_unique<Messages::Reduce>(state->m_destinationID, state->m_opType);
                txMsg->m_data = std::move(state->m_value);

                m_downPortTable.at(state->m_destinationID).pushOutgoing(std::move(txMsg));

                // Reset to-down state
                state.reset();
            }
        }
    }
}

void Edge::process(const size_t sourcePortIdx, std::unique_ptr<Messages::ReduceAll> msg)
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
                for(size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
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
                for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
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

void Edge::process(const size_t sourcePortIdx, std::unique_ptr<Messages::Scatter> msg)
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

    static const auto compNodeAmount = Network::Constants::deriveComputingNodeAmount();

    if(m_downPortTable.at(msg->m_sourceID.value()) != getPort(sourcePortIdx)) {
        spdlog::critical("Edge Switch({}): Source ID({}) and source port index({}) didn't match in scatter message!", m_ID, msg->m_sourceID.value(), sourcePortIdx);

        throw std::runtime_error("Edge Switch: Source ID and source port index didn't match in scatter message!");
    }

    if(0 != (msg->m_data.size() % compNodeAmount)) {
        spdlog::critical("Edge Switch({}): Scatter message size({}) is not divisible by the computing node amount({})!", m_ID, msg->m_data.size(), compNodeAmount);

        throw std::runtime_error("Edge Switch: Scatter message size is not divisible by the computing node amount!");
    }

    const auto chunkSize = msg->m_data.size() / compNodeAmount;

    std::vector<
        std::pair<
            size_t,
            decltype(msg->m_data)
        >
    > chunks;

    chunks.reserve(compNodeAmount);

    for(size_t compNodeIdx = 0; compNodeIdx < compNodeAmount; ++compNodeIdx) {
        if(msg->m_sourceID.value() == compNodeIdx) {
            continue; // Omit the source as it already extracted its own chunk
        }

        const auto chunkBegin = msg->m_data.cbegin() + (compNodeIdx * chunkSize);
        const auto chunkEnd   = chunkBegin + chunkSize;

        chunks.emplace_back(compNodeIdx, std::move(std::vector<float>(chunkBegin, chunkEnd)));
    }

    // Scatter to other down-ports
    {
        for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
            if(getDownPort(downPortIdx) == getPort(sourcePortIdx)) {
                continue;
            }

            auto uniqueMsg = std::make_unique<Network::Messages::Scatter>(msg->m_sourceID.value());

            auto pChunk = std::find_if(chunks.begin(), chunks.end(), [compNodeIdx = firstCompNodeIdx + downPortIdx](const auto& entry) { return entry.first == compNodeIdx; });

            if(pChunk == chunks.end()) {
                spdlog::critical("Edge Switch({}): Chunk for down-port #{} (i.e. computing node #{}) not found!", m_ID, downPortIdx, firstCompNodeIdx + downPortIdx);

                throw std::runtime_error("Edge Switch: Chunk for down-port not found!");
            }

            spdlog::trace("Edge Switch({}): Redirecting chunk for computing node #{} to down-port #{}..", m_ID, pChunk->first, downPortIdx);

            uniqueMsg->m_data = std::move(pChunk->second);
            chunks.erase(pChunk);

            getDownPort(downPortIdx).pushOutgoing(std::move(uniqueMsg));
        }
    }

    if(chunks.size() != (compNodeAmount - getDownPortAmount())) {
        spdlog::critical("Edge Switch({}): Chunks weren't distributed properly!", m_ID);

        throw std::runtime_error("Edge Switch: Chunks weren't distributed properly!");
    }

    // Re-direct the rest to aggregate switch
    {
        auto txMsg = std::make_unique<Messages::InterSwitch::Scatter>(msg->m_sourceID.value());

        txMsg->m_data = std::move(chunks);
        getAvailableUpPort().pushOutgoing(std::move(txMsg));
    }
}

void Edge::process(const size_t sourcePortIdx, [[maybe_unused]] std::unique_ptr<Messages::Gather> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    if(!msg->m_destinationID.has_value()) {
        spdlog::critical("Edge Switch({}): Gather message doesn't have a destination ID!", m_ID);

        throw std::runtime_error("Edge Switch: Gather message doesn't have a destination ID!");
    }

    if(sourcePortIdx < getUpPortAmount()) {
        spdlog::critical("Edge Switch({}): Received a gather message from an up-port!", m_ID);

        throw std::runtime_error("Edge Switch: Received a gather message from an up-port!");
    }

    // Decide on direction
    const bool bToUp = !isComputingNodeConnected(msg->m_destinationID.value());

    if(bToUp) {
        if(sourcePortIdx < getUpPortAmount()) { // Coming from an up-port
            spdlog::critical("Edge Switch({}): Received a gather message destined to up and from an up-port!", m_ID);

            throw std::runtime_error("Edge Switch: Received a gather message destined to up and from an up-port!");
        }

        auto &state = m_gatherStates.toUp;

        // Check if there was an ongoing transfer to down
        if(m_gatherStates.toDown.has_value()) {
            spdlog::critical("Edge Switch({}): Ongoing gather operation to down!", m_ID);

            throw std::runtime_error("Edge Switch: Ongoing gather operation to down!");
        }

        if(!state.has_value()) {
            state.emplace();

            state->m_destinationID = msg->m_destinationID.value();
            state->m_value.push_back({msg->m_sourceID.value(), std::move(msg->m_data)});
        }
        else {
            state->push(msg->m_sourceID.value(), msg->m_destinationID.value(), std::move(msg->m_data));

            // Check if all down-ports have sent message
            if(state->m_value.size() == getDownPortAmount()) {
                spdlog::trace("Edge Switch({}): Sending the gathered data to the same column up-port #{}", m_ID, m_sameColumnPortID);

                // Send gathered message to the same column up-port
                auto txMsg = std::make_unique<Messages::InterSwitch::Gather>(msg->m_destinationID.value());

                txMsg->m_data = std::move(state->m_value);

                getPort(m_sameColumnPortID).pushOutgoing(std::move(txMsg));

                state.reset();
            }
        }
    }
    else {
        auto &state = m_gatherStates.toDown;

        // Check if there was an ongoing transfer to up
        if(m_gatherStates.toUp.has_value()) {
            spdlog::critical("Edge Switch({}): Ongoing gather operation to up!", m_ID);

            throw std::runtime_error("Edge Switch: Ongoing gather operation to up!");
        }

        if(!state.has_value()) {
            state.emplace();

            state->m_destinationID = msg->m_destinationID.value();
            state->m_value.push_back({msg->m_sourceID.value(), std::move(msg->m_data)});
        }
        else {
            state->push(msg->m_sourceID.value(), msg->m_destinationID.value(), std::move(msg->m_data));

            if(state->m_value.size() == (Network::Constants::deriveComputingNodeAmount() - 1)) {
                auto txMsg = std::make_unique<Messages::Gather>(state->m_destinationID);

                std::sort(state->m_value.begin(), state->m_value.end(), [](const auto &a, const auto &b) {
                    return a.first < b.first;
                });

                txMsg->m_data.reserve(state->m_value.at(0).second.size() * state->m_value.size());

                for(auto &entry : state->m_value) {
                    txMsg->m_data.insert(txMsg->m_data.end(), std::make_move_iterator(entry.second.begin()), std::make_move_iterator(entry.second.end()));
                }
                state->m_value.clear();

                m_downPortTable.at(state->m_destinationID).pushOutgoing(std::move(txMsg));

                state.reset();
            }
        }
    }
}

void Edge::process(const size_t sourcePortIdx, std::unique_ptr<Messages::AllGather> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    spdlog::trace("Edge Switch({}): {} message received from port #{}", m_ID, msg->typeToString(), sourcePortIdx);

    if(msg->m_data.empty()) {
        spdlog::critical("Edge Switch({}): Received an empty {} message from source port #{}!", m_ID, msg->typeToString(), sourcePortIdx);

        throw std::runtime_error("Edge Switch: Received an empty all-gather message!");
    }

    if(sourcePortIdx < getUpPortAmount()) { // Coming from an up-port
        spdlog::critical("Edge Switch({}): {} message received from an up-port!", m_ID, msg->typeToString());

        throw std::runtime_error("Edge Switch: All-gather message received from an up-port!");
    }

    const auto sourceCompNodeIdx = firstCompNodeIdx + (sourcePortIdx - getUpPortAmount());
    auto &state = m_allGatherStates.toUp;

    if(state.bOngoing) {
        if(std::any_of(state.value.cbegin(), state.value.cend(), [sourceCompNodeIdx](const auto &entry) { return entry.first == sourceCompNodeIdx; })) {
            spdlog::critical("Edge Switch({}): This computing node({}) has already sent an all-gather message!", m_ID, sourceCompNodeIdx);

            throw std::runtime_error("Edge Switch: This port has already sent an all-gather message!");
        }

        state.value.push_back(std::make_pair(sourceCompNodeIdx, std::move(msg->m_data)));

        if(state.value.size() == getDownPortAmount()) {
            spdlog::debug("Edge Switch({}): All computing nodes have sent their all-gather messages..", m_ID);

            for(size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
                auto txMsg = std::make_unique<Messages::InterSwitch::AllGather>();

                txMsg->m_data.reserve(getDownPortAmount());
                txMsg->m_data = state.value;

                getUpPort(upPortIdx).pushOutgoing(std::move(txMsg));
            }

            // Reset state
            state.bOngoing = false;
            state.value.clear();
        }
    }
    else {
        spdlog::debug("Edge Switch({}): Initiating all-gather operation to-up..", m_ID);

        state.bOngoing = true;

        state.value.clear();
        state.value.reserve(getDownPortAmount()); // There will be down-port amount of messages to be re-directed
        state.value.push_back(std::make_pair(sourceCompNodeIdx, std::move(msg->m_data)));
    }
}

void Edge::process(const size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Reduce> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    if(!msg->m_destinationID.has_value()) {
        spdlog::critical("Edge({}): Reduce message doesn't have a destination ID!", m_ID);

        throw std::runtime_error("Edge: Reduce message doesn't have a destination ID!");
    }

    if(!isComputingNodeConnected(msg->m_destinationID.value())) {
        spdlog::critical("Edge({}): Destined computing #{} isn't connected to this switch!", m_ID, msg->m_destinationID.value());

        throw std::runtime_error("Edge: Destined computing node isn't connected to this switch!");
    }

    if(sourcePortIdx >= getUpPortAmount()) {
        spdlog::critical("Edge({}): Reduce message received from a down-port!", m_ID);

        throw std::runtime_error("Edge: Reduce message received from a down-port!");
    }

    auto &state = m_reduceStates.toDown;

    // Check if there was an ongoing transfer to up
    if(m_reduceStates.toUp.has_value()) {
        spdlog::critical("Edge Switch({}): Ongoing reduce operation to up!", m_ID);

        throw std::runtime_error("Edge Switch: Ongoing reduce operation to up!");
    }

    if(!state.has_value()) {
        state.emplace();

        state->m_destinationID = msg->m_destinationID.value();
        state->m_opType        = msg->m_opType;
        state->m_value         = std::move(msg->m_data);
        state->m_contributors  = std::move(msg->m_contributors);
    }
    else {
        state->push({msg->m_contributors}, msg->m_destinationID.value(), msg->m_opType, std::move(msg->m_data));

        if(state->m_contributors.size() == (Network::Constants::deriveComputingNodeAmount() - 1)) {
            auto txMsg = std::make_unique<Messages::Reduce>(state->m_destinationID, state->m_opType);
            txMsg->m_data = std::move(state->m_value);

            m_downPortTable.at(state->m_destinationID).pushOutgoing(std::move(txMsg));

            // Reset to-down state
            state.reset();
        }
    }
}

void Edge::process(const size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Scatter> msg)
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
            auto txMsg = std::make_unique<Messages::Scatter>(msg->m_sourceID.value());
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

void Edge::process(const size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Gather> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    if(!msg->m_destinationID.has_value()) {
        spdlog::critical("Edge({}): Gather message doesn't have a destination ID!", m_ID);

        throw std::runtime_error("Edge: Gather message doesn't have a destination ID!");
    }

    if(!isComputingNodeConnected(msg->m_destinationID.value())) {
        spdlog::critical("Edge({}): Destined computing #{} isn't connected to this switch!", m_ID, msg->m_destinationID.value());

        throw std::runtime_error("Edge: Destined computing node isn't connected to this switch!");
    }

    if(sourcePortIdx >= getUpPortAmount()) {
        spdlog::critical("Edge({}): Gather message received from a down-port!", m_ID);

        throw std::runtime_error("Edge: Gather message received from a down-port!");
    }

    auto &state = m_gatherStates.toDown;

    // Check if there was an ongoing transfer to up
    if(m_reduceStates.toUp.has_value()) {
        spdlog::critical("Edge Switch({}): Ongoing gather operation to up!", m_ID);

        throw std::runtime_error("Edge Switch: Ongoing gather operation to up!");
    }

    if(!state.has_value()) {
        state.emplace();

        state->m_destinationID = msg->m_destinationID.value();
        state->m_value         = std::move(msg->m_data);
    }
    else {
        for(auto &entry : msg->m_data) {
            state->push(entry.first, msg->m_destinationID.value(), std::move(entry.second));
        }

        if(state->m_value.size() == (Network::Constants::deriveComputingNodeAmount() - 1)) {
            auto txMsg = std::make_unique<Messages::Gather>(state->m_destinationID);

            std::sort(state->m_value.begin(), state->m_value.end(), [](const auto &a, const auto &b) {
                return a.first < b.first;
            });

            txMsg->m_data.reserve(state->m_value.at(0).second.size() * state->m_value.size());

            for(auto &entry : state->m_value) {
                txMsg->m_data.insert(txMsg->m_data.end(), std::make_move_iterator(entry.second.begin()), std::make_move_iterator(entry.second.end()));
            }
            state->m_value.clear();

            m_downPortTable.at(state->m_destinationID).pushOutgoing(std::move(txMsg));

            state.reset();
        }
    }
}

void Edge::process(const size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::AllGather> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    if(msg->m_data.empty()) {
        spdlog::critical("Edge({}): {} message cannot be empty!", m_ID, msg->typeToString());

        throw std::invalid_argument("Edge: All-gather message cannot be empty!");
    }

    if(sourcePortIdx >= getUpPortAmount()) {
        spdlog::critical("Edge({}): {} message received from down-port #{}!", m_ID, msg->typeToString(), sourcePortIdx - getUpPortAmount());

        throw std::runtime_error("Edge: All-gather message received from an down-port!");
    }

    auto &state = m_allGatherStates.toDown;

    if(state.bOngoing) {
        spdlog::trace("Edge({}): All-gather message received from port #{}", m_ID, sourcePortIdx);

        if(state.receiveFlags.at(sourcePortIdx)) {
            spdlog::critical("Edge({}): This port({}) has already sent an {} message!", m_ID, sourcePortIdx, msg->typeToString());

            throw std::runtime_error("Edge: This port has already sent an all-gather message!");
        }

        if(state.value != msg->m_data) {
            spdlog::critical("Edge({}): All-gather message data is different from the expected!", m_ID);

            throw std::runtime_error("Edge: All-gather message data is different from the expected!");
        }

        state.receiveFlags.at(sourcePortIdx) = true;

        if(std::all_of(state.receiveFlags.cbegin(), state.receiveFlags.cend(), [](const auto &entry) { return entry.second; })) {
            spdlog::debug("Edge({}): All ports have sent their {} messages..", m_ID, msg->typeToString());

            const size_t refDataSize = state.value.at(0).second.size();
            decltype(Messages::Gather::m_data) mergedData;
            for(size_t compNodeIdx = 0; compNodeIdx < Constants::deriveComputingNodeAmount(); ++compNodeIdx) {
                const auto dataPair = std::find_if(state.value.cbegin(), state.value.cend(), [compNodeIdx](const auto &entry) { return entry.first == compNodeIdx; });

                if(state.value.cend() == dataPair) {
                    spdlog::critical("Edge({}): Computing node #{} didn't send its all-gather message!", m_ID, compNodeIdx);
                    spdlog::debug("Edge({}): Gathered data size was {}", m_ID, state.value.size());

                    throw std::runtime_error("Edge: Computing node didn't send its all-gather message!");
                }

                const auto &compNodeData = dataPair->second;

                if(compNodeData.size() != refDataSize) {
                    spdlog::critical("Edge({}): All-gather message chunk data size is different from the expected!", m_ID);
                    spdlog::debug("Edge({}): Expected size {}, detected size for computing node #{} is {}", m_ID, refDataSize, compNodeIdx, compNodeData.size());

                    throw std::runtime_error("Edge: All-gather message data size is different from the expected!");
                }

                mergedData.insert(mergedData.end(), compNodeData.cbegin(), compNodeData.cend());
            }

            for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
                spdlog::trace("Edge({}): Preparing all-gather message for down-port #{}..", m_ID, downPortIdx);

                auto txMsg = std::make_unique<Messages::AllGather>();
                txMsg->m_data = mergedData;

                getDownPort(downPortIdx).pushOutgoing(std::move(txMsg));
            }

            // Reset state
            state.bOngoing = false;
            std::transform(state.receiveFlags.begin(),
                            state.receiveFlags.end(),
                            std::inserter(state.receiveFlags, state.receiveFlags.begin()),
                            [](auto &entry) { entry.second = false; return entry; });
            state.value.clear();
        }
    }
    else {
        spdlog::debug("Edge({}): Initiating all-gather operation to-down..", m_ID);

        if(m_allGatherStates.toDown.receiveFlags.size() != getUpPortAmount()) {
            spdlog::critical("Edge Switch({}): Amount of to-down all-gather requests is not equal to up-port amount!", m_ID);

            throw std::runtime_error("Invalid mapping!");
        }

        state.bOngoing = true;
        state.receiveFlags.at(sourcePortIdx) = true;
        state.value = std::move(msg->m_data);
    }
}

void Edge::redirect(const size_t sourcePortIdx, Network::Port::UniqueMsg msg)
{
    if(!msg) {
        spdlog::error("Edge Switch({}): Received null message for redirection!", m_ID);

        throw std::runtime_error("Null message for redirection!");
    }

    if(!msg->m_destinationID.has_value()) {
        spdlog::error("Edge Switch({}): Message {} doesn't have a destination ID!", m_ID, msg->typeToString());

        throw std::runtime_error("Message doesn't have a destination ID!");
    }

    // Decide on direction (up or down)
    if(auto search = m_downPortTable.find(msg->m_destinationID.value()); search != m_downPortTable.end()) {
        spdlog::trace("Edge Switch({}): Redirecting to a down-port..", m_ID);

        auto &targetPort = search->second;
        targetPort.pushOutgoing(std::move(msg));
    }
    else { // Re-direct to an up-port
        spdlog::trace("Edge Switch({}): Redirecting to an up-port..", m_ID);

        getAvailableUpPort().pushOutgoing(std::move(msg));
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

size_t Edge::getDownPortAmount() const
{
    static const size_t downPortAmount = m_portAmount / 2;

    return downPortAmount;
}

size_t Edge::getUpPortAmount() const
{
    static const size_t upPortAmount = m_portAmount / 2;

    return upPortAmount;
}

bool Edge::isComputingNodeConnected(const size_t compNodeIdx) const
{
    const size_t minIndex = firstCompNodeIdx;
    const size_t maxIndex = firstCompNodeIdx + getDownPortAmount() - 1;

    return ((compNodeIdx >= minIndex) && (compNodeIdx <= maxIndex));
}

void Edge::ReduceState::push(const std::vector<size_t> &sourceIDs, const size_t destID, const Messages::Reduce::OpType opType, decltype(m_value) &&data)
{
    if(opType != m_opType) {
        spdlog::critical("Edge Switch: Operation type mismatch in reduce messages! Expected {}, received {}", Messages::toString(m_opType), Messages::toString(opType));

        throw std::runtime_error("Edge Switch: Operation type mismatch in reduce messages!");
    }

    if(destID != m_destinationID) {
        spdlog::critical("Edge Switch: Destination IDs mismatch in reduce messages! Expected {}, received {}", m_destinationID, destID);

        throw std::runtime_error("Edge Switch: Destination IDs mismatch in reduce messages!");
    }

    for (const auto& sourceID : sourceIDs) {
        if (std::find(m_contributors.begin(), m_contributors.end(), sourceID) != m_contributors.end()) {
            spdlog::critical("Edge Switch: Source ID {} has already sent a reduce message!", sourceID);

            throw std::runtime_error("Edge Switch: Source ID has already sent a reduce message!");
        }
    }

    m_contributors.insert(m_contributors.end(), sourceIDs.begin(), sourceIDs.end());
    std::transform(m_value.cbegin(),
                   m_value.cend(),
                   data.cbegin(),
                   m_value.begin(),
                   [opType](const auto& lhs, const auto& rhs) { return Messages::reduce(lhs, rhs, opType); });
}

void Edge::GatherState::push(const size_t sourceID, const size_t destinationID, std::vector<float> &&data)
{
    if(destinationID != m_destinationID) {
        spdlog::critical("Edge Switch: Destination IDs mismatch in gather messages! Expected {}, received {}", m_destinationID, destinationID);

        throw std::runtime_error("Edge Switch: Destination IDs mismatch in gather messages!");
    }

    if(m_value.at(0).second.size() != data.size()) {
        spdlog::critical("Edge Switch: Data size mismatch in gather messages! Expected {}, received {}", m_value.at(0).second.size(), data.size());

        throw std::runtime_error("Edge Switch: Data size mismatch in gather messages!");
    }

    if(std::find_if(m_value.cbegin(), m_value.cend(), [sourceID](const auto &entry) { return (entry.first == sourceID); }) != m_value.cend()) {
        spdlog::critical("Edge Switch: Computing node #{} has already sent a gather message!", sourceID);

        throw std::runtime_error("Edge Switch: Computing node has already sent a gather message!");
    }

    m_value.push_back({sourceID, std::move(data)});
}
