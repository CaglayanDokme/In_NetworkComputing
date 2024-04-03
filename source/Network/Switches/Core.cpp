#include "Network/Derivations.hpp"
#include "spdlog/spdlog.h"
#include "Core.hpp"

using namespace Network::Switches;

Core::Core(const std::size_t portAmount)
: ISwitch(nextID++, portAmount)
{
    spdlog::trace("Created core switch with ID #{}", m_ID);

    // Initialize barrier requests
    for(std::size_t compNodeIdx = 0; compNodeIdx < Utilities::deriveComputingNodeAmount(m_portAmount); ++compNodeIdx) {
        m_barrierRequestFlags.insert({compNodeIdx, false});
    }

    // Initialize reduce requests
    for(std::size_t sourcePortIdx = 0; sourcePortIdx < m_portAmount; ++sourcePortIdx){
        m_reduceStates.flags.insert({sourcePortIdx, false});
    }

    // Initialize reduce-all requests
    for(std::size_t sourcePortIdx = 0; sourcePortIdx < m_portAmount; ++sourcePortIdx){
        m_reduceAllStates.flags.insert({sourcePortIdx, false});
    }

    if(compNodePerPort == 0) {
        compNodePerPort = std::size_t(std::pow(m_portAmount / 2, 2));
    }
}

bool Core::tick()
{
    // Advance all ports
    for(auto &port : m_ports) {
        port.tick();
    }

    // Check all ports for incoming messages
    for(size_t sourcePortIdx = 0; sourcePortIdx < m_ports.size(); ++sourcePortIdx) {
        auto &sourcePort = m_ports.at(sourcePortIdx);

        if(!sourcePort.hasIncoming()) {
            continue;
        }

        auto anyMsg = sourcePort.popIncoming();

        spdlog::trace("Core Switch({}): Received {} from sourcePort #{}.", m_ID, anyMsg->typeToString(), sourcePortIdx);

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
            default: {
                spdlog::error("Core Switch({}): Cannot determine the type of received message!", m_ID);
                spdlog::debug("Type name was {}", anyMsg->typeToString());

                return false;
            }
        }
    }

    return true;
}

void Core::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::DirectMessage> msg)
{
    spdlog::trace("Core Switch({}): {} destined to computing node #{}.", m_ID, msg->typeToString(), msg->m_destinationID);

    const auto targetPortIdx = msg->m_destinationID / compNodePerPort;
    spdlog::trace("Core Switch({}): Re-directing to port #{}..", m_ID, targetPortIdx);

    if(sourcePortIdx == targetPortIdx) {
        spdlog::critical("Core Switch({}): Target and source ports are the same({})!", m_ID, sourcePortIdx);

        throw std::runtime_error("Core Switch: Target and source ports are the same!");
    }

    m_ports.at(targetPortIdx).pushOutgoing(std::move(msg));
}

void Core::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Acknowledge> msg)
{
    spdlog::trace("Core Switch({}): {} destined to computing node #{}.", m_ID, msg->typeToString(), msg->m_destinationID);

    const auto targetPortIdx = msg->m_destinationID / compNodePerPort;
    spdlog::trace("Core Switch({}): Re-directing to port #{}..", m_ID, targetPortIdx);

    if(sourcePortIdx == targetPortIdx) {
        spdlog::critical("Core Switch({}): Target and source ports are the same({})!", m_ID, sourcePortIdx);

        throw std::runtime_error("Core Switch: Target and source ports are the same!");
    }

    m_ports.at(targetPortIdx).pushOutgoing(std::move(msg));
}

void Core::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::BroadcastMessage> msg)
{
    spdlog::trace("Core Switch({}): Broadcast message received from port #{}", m_ID, sourcePortIdx);

    // Re-direct to all other down-ports
    for(auto &port : m_ports) {
        if(getPort(sourcePortIdx) == port) {
            continue;
        }

        auto uniqueMsg = std::make_unique<Network::Messages::BroadcastMessage>(*msg);
        port.pushOutgoing(std::move(uniqueMsg));
    }
}

void Core::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::BarrierRequest> msg)
{
    // Process message
    {
        if(auto &barrierRequest = m_barrierRequestFlags.at(msg->m_sourceID); barrierRequest) {
            spdlog::warn("Core Switch({}): Computing node #{} already sent a barrier request!", m_ID, msg->m_sourceID);
        }
        else {
            barrierRequest = true;
        }
    }

    // Check for barrier release
    {
        if(std::all_of(m_barrierRequestFlags.cbegin(), m_barrierRequestFlags.cend(), [](const auto& entry) { return entry.second; })) {
            spdlog::trace("Core Switch({}): All computing nodes sent barrier requests, releasing the barrier..", m_ID);

            // Send barrier release to all ports
            for(auto &port: m_ports) {
                port.pushOutgoing(std::make_unique<Messages::BarrierRelease>());
            }

            // Reset all requests
            for(auto &elem: m_barrierRequestFlags) {
                elem.second = false;
            }
        }
    }
}

void Core::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Reduce> msg)
{
    // Process message
    {
        spdlog::trace("Core Switch({}): Received reduce message destined to computing node #{}.", m_ID, msg->m_destinationID);

        // Check if this is the first reduce message
        if(std::all_of(m_reduceStates.flags.cbegin(), m_reduceStates.flags.cend(), [](const auto& entry) { return !entry.second; })) {
            m_reduceStates.destinationID = msg->m_destinationID;
            m_reduceStates.opType        = msg->m_opType;
            m_reduceStates.value         = std::move(msg->m_data);
            m_reduceStates.flags.at(sourcePortIdx) = true;
        }
        else {
            if(m_reduceStates.flags.at(sourcePortIdx)) {
                spdlog::critical("Core Switch({}): Received multiple reduce messages from port #{}!", m_ID, sourcePortIdx);

                throw std::runtime_error("Core Switch: Received multiple reduce messages!");
            }

            if(msg->m_opType != m_reduceStates.opType) {
                spdlog::critical("Core Switch({}): Wrong reduce operation type from port #{}! Expected {}, got {}", m_ID, sourcePortIdx, Messages::toString(m_reduceStates.opType), Messages::toString(msg->m_opType));

                throw std::runtime_error("Core Switch: Operation types doesn't match in reduce messages!");
            }

            m_reduceStates.flags.at(sourcePortIdx) = true;
            std::transform(m_reduceStates.value.cbegin(),
                            m_reduceStates.value.cend(),
                            msg->m_data.cbegin(),
                            m_reduceStates.value.begin(),
                            [opType = m_reduceStates.opType](const auto& lhs, const auto& rhs) { return Messages::reduce(lhs, rhs, opType); });
        }
    }

    // Check if all reduce messages are received
    {
        const auto rxCount = std::count_if(m_reduceStates.flags.cbegin(), m_reduceStates.flags.cend(), [](const auto& entry) { return entry.second; });
        if((m_reduceStates.flags.size() - 1) == rxCount) {
            const auto targetPortIdx = m_reduceStates.destinationID / compNodePerPort;
            spdlog::trace("Core Switch({}): Sending reduce message destined to computing node #{} to port #{}", m_ID, m_reduceStates.destinationID, targetPortIdx);

            if(m_reduceStates.flags.at(targetPortIdx)) {
                spdlog::critical("Core Switch({}): Target port(#{}) was actually a source port!", m_ID, targetPortIdx);

                throw std::runtime_error("Core Switch: Target port was actually a source port!");
            }

            auto msg = std::make_unique<Messages::Reduce>(m_reduceStates.destinationID, m_reduceStates.opType);
            msg->m_data = std::move(m_reduceStates.value);
            m_reduceStates.value.clear();

            m_ports.at(targetPortIdx).pushOutgoing(std::move(msg));

            std::transform(m_reduceStates.flags.begin(),
                            m_reduceStates.flags.end(),
                            std::inserter(m_reduceStates.flags, m_reduceStates.flags.begin()),
                            [](auto& entry) { entry.second = false; return entry; });

            if(m_reduceStates.flags.size() != m_portAmount) {
                spdlog::critical("Core Switch({}): Reduce flag map corrupted, size is {}!", m_ID, m_reduceStates.flags.size());

                throw std::runtime_error("Core Switch: Reduce flag map corrupted!");
            }
        }
    }
}

void Core::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::ReduceAll> msg)
{
    // Process message
    {
        spdlog::trace("Core Switch({}): Received reduce-all message from port #{}.", m_ID, sourcePortIdx);

        // Check if this is the first reduce-all message
        if(!m_reduceAllStates.bOngoing) {
            m_reduceAllStates.bOngoing = true;
            m_reduceAllStates.opType = msg->m_opType;
            m_reduceAllStates.value  = std::move(msg->m_data);
            m_reduceAllStates.flags.at(sourcePortIdx) = true;
        }
        else {
            if(m_reduceAllStates.flags.at(sourcePortIdx)) {
                spdlog::critical("Core Switch({}): Received multiple reduce-all messages from port #{}!", m_ID, sourcePortIdx);

                throw std::runtime_error("Core Switch: Received multiple reduce-all messages!");
            }

            if(msg->m_opType != m_reduceAllStates.opType) {
                spdlog::critical("Core Switch({}): Wrong reduce-all operation type from port #{}! Expected {}, got {}", m_ID, sourcePortIdx, Messages::toString(m_reduceAllStates.opType), Messages::toString(msg->m_opType));

                throw std::runtime_error("Core Switch: Operation types doesn't match in reduce-all messages!");
            }

            m_reduceAllStates.flags.at(sourcePortIdx) = true;
            std::transform(m_reduceAllStates.value.cbegin(),
                            m_reduceAllStates.value.cend(),
                            msg->m_data.cbegin(),
                            m_reduceAllStates.value.begin(),
                            [opType = m_reduceAllStates.opType](const auto& lhs, const auto& rhs) { return Messages::reduce(lhs, rhs, opType); });
        }
    }

    // Check if all ports received the message
    {
        const auto rxCount = std::count_if(m_reduceAllStates.flags.cbegin(), m_reduceAllStates.flags.cend(), [](const auto& entry) { return entry.second; });
        if(m_reduceAllStates.flags.size() == rxCount) {
            // Send reduce-all result message to all down-ports
            for(auto &port: m_ports) {
                auto msg = std::make_unique<Messages::ReduceAll>(m_reduceAllStates.opType);
                msg->m_data = m_reduceAllStates.value;

                port.pushOutgoing(std::move(msg));
            }

            // Reset all requests
            for(auto &elem: m_reduceAllStates.flags) {
                elem.second = false;
            }

            m_reduceAllStates.bOngoing = false;
        }
    }
}

void Core::process(const std::size_t sourcePortIdx, std::unique_ptr<Messages::Scatter> msg)
{
    spdlog::trace("Core Switch({}): Scatter message received from port #{}", m_ID, sourcePortIdx);

    if(msg->m_data.empty()) {
        spdlog::critical("Core Switch({}): Received empty scatter message from port #{}!", m_ID, sourcePortIdx);

        throw std::runtime_error("Core Switch: Received empty scatter message!");
    }

    const std::size_t targetPortAmount = m_portAmount - 1;

    if((msg->m_data.size() % targetPortAmount) != 0) {
        spdlog::critical("Core Switch({}): Scatter message size({}) is not divisible by target port amount({})!", m_ID, msg->m_data.size(), targetPortAmount);

        throw std::runtime_error("Core Switch: Scatter message size is not divisible by target port amount!");
    }

    const std::size_t chunkSize = msg->m_data.size() / targetPortAmount;

    for(std::size_t targetPortIdx = 0, dataIdx = 0; targetPortIdx < m_portAmount; ++targetPortIdx) {
        if(sourcePortIdx == targetPortIdx) {
            spdlog::trace("Core Switch({}): Skipping source port #{}..", m_ID, sourcePortIdx);

            continue;
        }

        spdlog::trace("Core Switch({}): Redirecting section [{}, {}) to down-port #{}", m_ID, dataIdx, dataIdx + chunkSize, targetPortIdx);

        auto scatterMsg = std::make_unique<Messages::Scatter>(msg->m_sourceID);
        scatterMsg->m_data.assign(msg->m_data.cbegin() + dataIdx, msg->m_data.cbegin() + dataIdx + chunkSize);

        m_ports.at(targetPortIdx).pushOutgoing(std::move(scatterMsg));

        dataIdx += chunkSize;
    }
}
