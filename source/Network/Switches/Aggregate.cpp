#include "Aggregate.hpp"
#include "spdlog/spdlog.h"
#include "Network/Message.hpp"
#include "Network/Constants.hpp"

using namespace Network::Switches;

Aggregate::Aggregate(const std::size_t portAmount)
: ISwitch(nextID++, portAmount), assocCompNodeAmount(getDownPortAmount() * getDownPortAmount()), firstCompNodeIdx((m_ID / getDownPortAmount()) * assocCompNodeAmount)
{
    spdlog::trace("Created aggregate switch with ID #{}", m_ID);

    // Calculate look-up table for re-direction to down ports
    {
        const std::size_t downPortAmount = getDownPortAmount();

        for(size_t downPortIdx = 0; downPortIdx < downPortAmount; ++downPortIdx) {
            for(size_t edgeDownPortIdx = 0; edgeDownPortIdx < downPortAmount; ++edgeDownPortIdx) {
                const auto compNodeIdx = firstCompNodeIdx + (downPortIdx * downPortAmount) + edgeDownPortIdx;
                m_downPortTable.insert({compNodeIdx, getDownPort(downPortIdx)});

                spdlog::trace("Aggregate Switch({}): Mapped computing node #{} with down port #{}.", m_ID, compNodeIdx, downPortIdx);
            }
        }
    }

    // Initialize barrier flags
    {
        // Request flags
        for(std::size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
            m_barrierRequestFlags.insert({downPortIdx, false});
        }

        // Release flags
        for(std::size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
            m_barrierReleaseFlags.insert({upPortIdx, false});
        }
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

    // Initialize all-gather state
    {
        // To-up
        {
            // Down-port receive flags
            for(std::size_t portIdx = getUpPortAmount(); portIdx < m_portAmount; ++portIdx) {
                m_allGatherStates.toUp.receiveFlags.insert({portIdx, false});
            }

            if(m_allGatherStates.toUp.receiveFlags.size() != getDownPortAmount()) {
                spdlog::critical("Edge Switch({}): Amount of up-port all-gather requests is not equal to down-port amount!", m_ID);
                spdlog::debug("Aggregate Switch({}): Expected {}, got {}", m_ID, getUpPortAmount(), m_allGatherStates.toUp.receiveFlags.size());

                throw std::runtime_error("Invalid mapping!");
            }
        }

        // To-down
        {
            // Up-port receive flags
            for(std::size_t portIdx = 0; portIdx < getUpPortAmount(); ++portIdx) {
                m_allGatherStates.toDown.receiveFlags.insert({portIdx, false});
            }

            if(m_allGatherStates.toDown.receiveFlags.size() != getUpPortAmount()) {
                spdlog::critical("Edge Switch({}): Amount of down-port all-gather requests is not equal to up-port amount!", m_ID);
                spdlog::debug("Aggregate Switch({}): Expected {}, got {}", m_ID, getDownPortAmount(), m_allGatherStates.toDown.receiveFlags.size());

                throw std::runtime_error("Invalid mapping!");
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

        spdlog::trace("Aggregate Switch({}): Received {} from sourcePort #{}.", m_ID, anyMsg->typeToString(), sourcePortIdx);

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
                spdlog::error("Aggregate Switch({}): Cannot determine the type of received message!", m_ID);
                spdlog::debug("Type name was {}", anyMsg->typeToString());

                return false;
            }
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

void Aggregate::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::DirectMessage> msg)
{
    redirect(sourcePortIdx, std::move(msg));
}

void Aggregate::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Acknowledge> msg)
{
    spdlog::trace("Aggregate Switch({}): Acknowledge received from port #{} destined to computing node #{}.", m_ID, sourcePortIdx, msg->m_destinationID.value());

    // Decide on direction
    if(auto search = m_downPortTable.find(msg->m_destinationID.value()); search != m_downPortTable.end()) {
        spdlog::trace("Aggregate Switch({}): Redirecting to a down-port..", m_ID);

        auto &targetPort = search->second;

        targetPort.pushOutgoing(std::move(msg));
    }
    else { // Re-direct to up-port(s)
        spdlog::trace("Aggregate Switch({}): Redirecting to an up-port..", m_ID);

        getAvailableUpPort().pushOutgoing(std::move(msg));
    }
}

void Aggregate::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::BroadcastMessage> msg)
{
    spdlog::trace("Aggregate Switch({}): Broadcast message received from port #{}", m_ID, sourcePortIdx);

    // Decide on direction
    if(sourcePortIdx >= getUpPortAmount()) { // Coming from a down-port
        spdlog::trace("Aggregate Switch({}): Redirecting to other down-ports..", m_ID);

        for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
            auto &targetPort = getDownPort(downPortIdx);

            if(getPort(sourcePortIdx) != targetPort) {
                auto uniqueMsg = std::make_unique<Network::Messages::BroadcastMessage>(*msg);
                targetPort.pushOutgoing(std::move(uniqueMsg));
            }
        }

        // Re-direct to up-port with minimum messages in it
        {
            spdlog::trace("Aggregate Switch({}): Redirecting to an up-port..", m_ID);

            // Avoid copying for the last re-direction as we no more need the message content
            getAvailableUpPort().pushOutgoing(std::move(msg));
        }
    }
    else { // Coming from an up-port
        spdlog::trace("Aggregate Switch({}): Redirecting to all down-ports..", m_ID);

        // Re-direct to all down-ports
        for(size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
            auto uniqueMsg = std::make_unique<Network::Messages::BroadcastMessage>(*msg);
            getDownPort(downPortIdx).pushOutgoing(std::move(uniqueMsg));
        }
    }
}

void Aggregate::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::BarrierRequest> msg)
{
    if(!msg) {
        spdlog::critical("Edge({}): Null message given!", m_ID);

        throw std::invalid_argument("Edge: Null message given!");
    }

    if(sourcePortIdx < getUpPortAmount()) { // Coming from an up-port
        spdlog::critical("Aggregate Switch({}): Barrier request received from an up-port!", m_ID);
        spdlog::debug("Aggregate Switch({}): Source ID was #{}!", m_ID, msg->m_sourceID.value());

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

void Aggregate::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::BarrierRelease> msg)
{
    if(sourcePortIdx >= getUpPortAmount()) { // Coming from a down-port
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
                getDownPort(downPortIdx).pushOutgoing(std::move(std::make_unique<Network::Messages::BarrierRelease>()));
            }

            // Reset all flags
            for(auto &entry : m_barrierReleaseFlags) {
                entry.second = false;
            }
        }
    }
}

void Aggregate::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Reduce> msg)
{
    // Check if source port exists in source table for reduction messages
    if(std::find_if(m_reduceStates.flags.cbegin(), m_reduceStates.flags.cend(), [sourcePortIdx](const auto& entry) { return entry.first == sourcePortIdx; }) == m_reduceStates.flags.cend()) {
        spdlog::error("Aggregate Switch({}): Received a reduction message from an invalid source port(#{})!", m_ID, sourcePortIdx);

        throw std::runtime_error("Received a reduction message from an invalid source port!");
    }

    const bool bDownPort = (sourcePortIdx >= getUpPortAmount());
    if(bDownPort) {
        // A reduce message can be received from the same column down-port only
        if(sourcePortIdx != m_reduceStates.sameColumnPortID) {
            spdlog::error("Aggregate Switch({}): Received a reduce message from an invalid down-port(#{})!", m_ID, sourcePortIdx);

            throw std::runtime_error(" Received a reduce message from an invalid down-port!");
        }
    }

    const bool bOngoingRedirection = !std::all_of(m_reduceStates.flags.cbegin(), m_reduceStates.flags.cend(), [](const auto& entry) { return !entry.second; });

    // Decide on direction (up or down)
    auto search = m_downPortTable.find(msg->m_destinationID.value());

    if((search != m_downPortTable.end())) {
        if(bOngoingRedirection) {
            // Process message
            {
                if(m_reduceStates.flags.at(sourcePortIdx)) {
                    spdlog::critical("Aggregate Switch({}): Received multiple reduce messages from port #{}!", m_ID, sourcePortIdx);

                    throw std::runtime_error("Aggregate Switch: Received multiple reduce messages!");
                }

                if(msg->m_opType != m_reduceStates.opType) {
                    spdlog::critical("Aggregate Switch({}): Wrong reduce operation type from port #{}! Expected {}, got {}", m_ID, sourcePortIdx, Messages::toString(m_reduceStates.opType), Messages::toString(msg->m_opType));

                    throw std::runtime_error("Aggregate Switch: Operation types doesn't match in reduce messages!");
                }

                if(m_reduceStates.value.size() != msg->m_data.size()) {
                    spdlog::critical("Aggregate Switch({}): Received reduce message with different data size from port #{}! Expected {}, got {}", m_ID, sourcePortIdx, m_reduceStates.value.size(), msg->m_data.size());

                    throw std::runtime_error("Aggregate Switch: Data sizes doesn't match in reduce messages!");
                }

                const bool bFirstUpPortData = !bDownPort && m_reduceStates.upPortReferenceValue.empty();

                if(bDownPort || bFirstUpPortData) {
                    std::transform( m_reduceStates.value.cbegin(),
                                    m_reduceStates.value.cend(),
                                    msg->m_data.cbegin(),
                                    m_reduceStates.value.begin(),
                                    [opType = m_reduceStates.opType](const auto& lhs, const auto& rhs) { return Messages::reduce(lhs, rhs, opType); });
                }

                m_reduceStates.flags.at(sourcePortIdx) = true;

                if(bFirstUpPortData) {
                    m_reduceStates.upPortReferenceValue = std::move(msg->m_data);
                }
                else {
                    if(m_reduceStates.upPortReferenceValue != msg->m_data) {
                        spdlog::critical("Aggregate Switch({}): Up-port(#{}) value doesn't match the reference value!", m_ID, sourcePortIdx);

                        throw std::runtime_error("Aggregate Switch: Up-port value doesn't match the reference value!");
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

                        auto msg = std::make_unique<Messages::Reduce>(m_reduceStates.destinationID, m_reduceStates.opType);
                        msg->m_data = std::move(m_reduceStates.value);
                        m_reduceStates.value.clear();
                        m_reduceStates.upPortReferenceValue.clear();

                        destinationPort.pushOutgoing(std::move(msg));

                        std::transform(m_reduceStates.flags.begin(),
                                        m_reduceStates.flags.end(),
                                        std::inserter(m_reduceStates.flags, m_reduceStates.flags.begin()),
                                        [](auto& entry) { entry.second = false; return entry; });
                    }
                }
                else {
                    if(m_reduceStates.flags.size() == rxCount) { // i.e. getUpPortAmount() + 1
                        spdlog::trace("Aggregate Switch({}): All reduce messages received from all up-ports and the same column down-port! Re-directing..", m_ID);

                        auto msg = std::make_unique<Messages::Reduce>(m_reduceStates.destinationID, m_reduceStates.opType);
                        msg->m_data = std::move(m_reduceStates.value);
                        m_reduceStates.value.clear();
                        m_reduceStates.upPortReferenceValue.clear();

                        destinationPort.pushOutgoing(std::move(msg));

                        std::transform(m_reduceStates.flags.begin(),
                                        m_reduceStates.flags.end(),
                                        std::inserter(m_reduceStates.flags, m_reduceStates.flags.begin()),
                                        [](auto& entry) { entry.second = false; return entry; });
                    }
                }
            }
        }
        else {
            m_reduceStates.destinationID = msg->m_destinationID.value();
            m_reduceStates.opType        = msg->m_opType;
            m_reduceStates.value         = std::move(msg->m_data);

            if(!bDownPort) {
                if(!m_reduceStates.upPortReferenceValue.empty()) {
                    spdlog::critical("Aggregate Switch({}): Up-port reference value must have been empty!", m_ID);

                    throw std::runtime_error("Aggregate Switch: Up-port reference value must have been empty!");
                }

                m_reduceStates.upPortReferenceValue = m_reduceStates.value;
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
            getUpPort(upPortIdx).pushOutgoing(std::make_unique<Messages::Reduce>(*msg));
        }
    }
}

void Aggregate::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::ReduceAll> msg)
{
    const bool bDownPort = (sourcePortIdx >= getUpPortAmount());

    if(bDownPort) {
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
                spdlog::critical("Edge Switch({}): Wrong reduce-all operation type from port #{}! Expected {}, got {}", m_ID, sourcePortIdx, Messages::toString(state.opType), Messages::toString(msg->m_opType));

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

                // Reset to-up state
                state.bOngoing = false;
                state.value.clear();
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
                state.value.clear();
                std::transform(state.receiveFlags.begin(),
                                state.receiveFlags.end(),
                                std::inserter(state.receiveFlags, state.receiveFlags.begin()),
                                [](auto& entry) { entry.second = false; return entry; });
            }
        }
    }
}

void Aggregate::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Scatter> msg)
{
    static const auto compNodeAmount = Network::Constants::deriveComputingNodeAmount();
    static const auto downPortAmount = getDownPortAmount();

    spdlog::trace("Aggregate Switch({}): Scatter message received from port #{}", m_ID, sourcePortIdx);

    if(msg->m_data.empty()) {
        spdlog::critical("Aggregate Switch({}): Received an empty scatter message from source port #{}!", m_ID, sourcePortIdx);

        throw std::runtime_error("Aggregate Switch: Received an empty scatter message!");
    }

    // Decide on direction
    if(sourcePortIdx < getUpPortAmount()) { // Coming from an up-port
        if(msg->m_data.size() != assocCompNodeAmount) {
            spdlog::critical("Aggregate Switch({}): Scatter message size({}) is not equal to associated computing node amount({})!", m_ID, msg->m_data.size(), assocCompNodeAmount);

            throw std::runtime_error("Aggregate Switch: Scatter message size is not equal to associated computing node amount!");
        }

        // Redirect to down-ports (e.g. edge switches)
        for(size_t downPortIdx = 0; downPortIdx < downPortAmount; ++downPortIdx) {
            auto txMsg = std::make_unique<Messages::InterSwitch::Scatter>(msg->m_sourceID.value());
            txMsg->m_data.resize(downPortAmount); // Each edge switch has down-port amount of computing nodes

            const auto localFirstCompNodeIdx = firstCompNodeIdx + (downPortIdx * downPortAmount);
            for(size_t compNodeIdx = localFirstCompNodeIdx; compNodeIdx < (localFirstCompNodeIdx + downPortAmount); ++compNodeIdx) {
                auto iterator = std::find_if(msg->m_data.begin(), msg->m_data.end(), [compNodeIdx](const auto& entry) { return entry.first == compNodeIdx; });
                if(msg->m_data.end() == iterator) {
                    spdlog::critical("Aggregate Switch({}): Computing node #{} is not found in the scatter message!", m_ID, compNodeIdx);

                    throw std::runtime_error("Aggregate Switch: Computing node is not found in the scatter message!");
                }

                txMsg->m_data.at(compNodeIdx - localFirstCompNodeIdx).first = compNodeIdx;
                txMsg->m_data.at(compNodeIdx - localFirstCompNodeIdx).second = std::move(iterator->second);
                msg->m_data.erase(iterator); // Accelerate the search for the next computing node
            }

            getDownPort(downPortIdx).pushOutgoing(std::move(txMsg));
        }
    }
    else { // Coming from a down-port
        const auto expectedSize = compNodeAmount - (assocCompNodeAmount - downPortAmount); // Each edge switch has down-port amount of computing nodes

        if(msg->m_data.size() != expectedSize) {
            spdlog::critical("Aggregate Switch({}): Scatter message size({}) is not equal to expected size({})!", m_ID, msg->m_data.size(), expectedSize);

            throw std::runtime_error("Aggregate Switch: Scatter message size is not equal to expected size!");
        }

        // Redirect to other down-port(s) (e.g. edge switches)
        for(size_t downPortIdx = 0; downPortIdx < downPortAmount; ++downPortIdx) {
            if(downPortIdx == (sourcePortIdx - getUpPortAmount())) {
                continue;
            }

            auto txMsg = std::make_unique<Messages::InterSwitch::Scatter>(msg->m_sourceID.value());
            txMsg->m_data.resize(downPortAmount); // Each edge switch has down-port amount of computing nodes

            const auto localFirstCompNodeIdx = firstCompNodeIdx + (downPortIdx * downPortAmount);
            for(size_t compNodeIdx = localFirstCompNodeIdx; compNodeIdx < (localFirstCompNodeIdx + downPortAmount); ++compNodeIdx) {
                auto iterator = std::find_if(msg->m_data.begin(), msg->m_data.end(), [compNodeIdx](const auto& entry) { return entry.first == compNodeIdx; });
                if(msg->m_data.end() == iterator) {
                    spdlog::critical("Aggregate Switch({}): Computing node #{} is not found in the scatter message!", m_ID, compNodeIdx);

                    throw std::runtime_error("Aggregate Switch: Computing node is not found in the scatter message!");
                }

                txMsg->m_data.at(compNodeIdx - localFirstCompNodeIdx).first = compNodeIdx;
                txMsg->m_data.at(compNodeIdx - localFirstCompNodeIdx).second = std::move(iterator->second);
                msg->m_data.erase(iterator); // Extract the redirected content
            }

            getDownPort(downPortIdx).pushOutgoing(std::move(txMsg));
        }

        // Redirect to up-port with minimum messages in it (Down-port content already extracted)
        getAvailableUpPort().pushOutgoing(std::move(msg));
    }
}

void Aggregate::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::Gather> msg)
{
    spdlog::trace("Aggregate Switch({}): Gather message received from port #{}", m_ID, sourcePortIdx);

    if(msg->m_data.empty()) {
        spdlog::critical("Aggregate Switch({}): Received an empty gather message from source port #{}!", m_ID, sourcePortIdx);

        throw std::runtime_error("Aggregate Switch: Received an empty gather message!");
    }

    const bool bDownPort = (sourcePortIdx >= getUpPortAmount());

    if(bDownPort) {
        // Decide on direction (up or down)
        if(auto search = m_downPortTable.find(msg->m_destinationID.value()); search != m_downPortTable.end()) {
            spdlog::trace("Aggregate Switch({}): Redirecting to a down-port..", m_ID);

            if(getPort(sourcePortIdx) == search->second) {
                spdlog::critical("Aggregate Switch({}): Gather message is destined to the same down-port!", m_ID);

                throw std::runtime_error("Aggregate Switch: Gather message is destined to the same down-port!");
            }

            search->second.pushOutgoing(std::move(msg));
        }
        else {
            spdlog::trace("Aggregate Switch({}): Redirecting to an up-port..", m_ID);

            getAvailableUpPort().pushOutgoing(std::move(msg));
        }
    }
    else {
        auto search = m_downPortTable.find(msg->m_destinationID.value());

        if(search == m_downPortTable.end()) {
            spdlog::critical("Aggregate Switch({}): Gather message is not destined to a down-port!", m_ID);

            throw std::runtime_error("Aggregate Switch: Gather message is not destined to a down-port!");
        }

        search->second.pushOutgoing(std::move(msg));
    }
}

void Aggregate::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::InterSwitch::AllGather> msg)
{
    if(!msg) {
        spdlog::critical("Aggregate Switch({}): Received an empty {} message from source port #{}!", m_ID, msg->typeToString(), sourcePortIdx);

        throw std::runtime_error("Aggregate Switch: Received an empty message!");
    }

    spdlog::trace("Aggregate Switch({}): {} message received from port #{}", m_ID, msg->typeToString(), sourcePortIdx);

    const bool bDownPort = (sourcePortIdx >= getUpPortAmount());

    if(bDownPort) {
        auto &state = m_allGatherStates.toUp;

        if(state.bOngoing) {
            if((state.value.size() % msg->m_data.size()) != 0) {
                spdlog::critical("Aggregate Switch({}): Received a {} message with invalid length from source port #{}!", m_ID, msg->typeToString(), sourcePortIdx);
                spdlog::debug("Aggregate Switch({}): Expected length was a multiple of {}, got {} (Expected might be wrong as well!)", m_ID, msg->m_data.size(), state.value.size());

                throw std::runtime_error("Aggregate Switch: Received an all-gather message with invalid length!");
            }

            state.value.insert(state.value.end(), msg->m_data.cbegin(), msg->m_data.cend());
            state.receiveFlags.at(sourcePortIdx) = true;

            if(std::all_of(state.receiveFlags.cbegin(), state.receiveFlags.cend(), [](const auto& entry) { return entry.second; })) {
                spdlog::trace("Aggregate Switch({}): All-gather message received from all down-ports! Re-directing..", m_ID);

                for(std::size_t upPortIdx = 0; upPortIdx < getUpPortAmount(); ++upPortIdx) {
                    auto txMsg = std::make_unique<Messages::InterSwitch::AllGather>();
                    txMsg->m_data = state.value;

                    getUpPort(upPortIdx).pushOutgoing(std::move(txMsg));
                }

                state.bOngoing = false;
                state.value.clear();
                std::transform(state.receiveFlags.begin(),
                                state.receiveFlags.end(),
                                std::inserter(state.receiveFlags, state.receiveFlags.begin()),
                                [](auto& entry) { entry.second = false; return entry; });
            }
        }
        else {
            if(state.receiveFlags.size() != getDownPortAmount()) {
                spdlog::critical("Aggregate Switch({}): Amount of up-port all-gather requests is not equal to down-port amount!", m_ID);
                spdlog::debug("Aggregate Switch({}): Expected {}, got {}", m_ID, getDownPortAmount(), state.receiveFlags.size());

                throw std::runtime_error("Aggregate Switch: Invalid mapping!");
            }

            state.bOngoing = true;
            state.value = std::move(msg->m_data);
            state.receiveFlags.at(sourcePortIdx) = true;
        }
    }
    else {
        auto &state = m_allGatherStates.toDown;

        if(state.bOngoing) {
            if((state.value.size() != msg->m_data.size())) {
                spdlog::critical("Aggregate Switch({}): Received a {} message with invalid length from source port #{}!", m_ID, msg->typeToString(), sourcePortIdx);
                spdlog::debug("Aggregate Switch({}): Expected length was {}, got {}", m_ID, state.value.size(), msg->m_data.size());

                throw std::runtime_error("Aggregate Switch: Received an all-gather message with invalid length!");
            }

            if(state.value != msg->m_data) {
                spdlog::critical("Aggregate Switch({}): Received a {} message with different data from source port #{}!", m_ID, msg->typeToString(), sourcePortIdx);

                throw std::runtime_error("Aggregate Switch: Received an all-gather message with different data!");
            }

            state.receiveFlags.at(sourcePortIdx) = true;

            if(std::all_of(state.receiveFlags.cbegin(), state.receiveFlags.cend(), [](const auto& entry) { return entry.second; })) {
                spdlog::trace("Aggregate Switch({}): All-gather message received from all up-ports! Re-directing..", m_ID);

                for(std::size_t downPortIdx = 0; downPortIdx < getDownPortAmount(); ++downPortIdx) {
                    auto txMsg = std::make_unique<Messages::InterSwitch::AllGather>();
                    txMsg->m_data = state.value;

                    getDownPort(downPortIdx).pushOutgoing(std::move(txMsg));
                    state.receiveFlags.at(downPortIdx) = false;
                }

                state.bOngoing = false;
                state.value.clear();
                std::transform(state.receiveFlags.begin(),
                                state.receiveFlags.end(),
                                std::inserter(state.receiveFlags, state.receiveFlags.begin()),
                                [](auto& entry) { entry.second = false; return entry; });
            }
        }
        else {
            if(state.receiveFlags.size() != getUpPortAmount()) {
                spdlog::critical("Aggregate Switch({}): Amount of down-port all-gather requests is not equal to up-port amount!", m_ID);
                spdlog::debug("Aggregate Switch({}): Expected {}, got {}", m_ID, getUpPortAmount(), state.receiveFlags.size());

                throw std::runtime_error("Aggregate Switch: Invalid mapping!");
            }

            state.bOngoing = true;
            state.value = std::move(msg->m_data);
            state.receiveFlags.at(sourcePortIdx) = true;
        }
    }
}

void Aggregate::redirect(const std::size_t sourcePortIdx, Network::Port::UniqueMsg msg)
{
    if(!msg) {
        spdlog::error("Aggregate Switch({}): Received null message for redirection!", m_ID);

        throw std::runtime_error("Null message for redirection!");
    }

    if(!msg->m_destinationID.has_value()) {
        spdlog::error("Aggregate Switch({}): Message {} doesn't have a destination ID!", m_ID, msg->typeToString());

        throw std::runtime_error("Message doesn't have a destination ID!");
    }

    // Decide on direction (up or down)
    if(auto search = m_downPortTable.find(msg->m_destinationID.value()); search != m_downPortTable.end()) {
        spdlog::trace("Aggregate Switch({}): Redirecting to a down-port..", m_ID);

        auto &targetPort = search->second;

        targetPort.pushOutgoing(std::move(msg));
    }
    else { // Re-direct to up-port(s)
        spdlog::trace("Aggregate Switch({}): Redirecting to an up-port..", m_ID);

        getAvailableUpPort().pushOutgoing(std::move(msg));
    }
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
