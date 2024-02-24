#include "Network/Derivations.hpp"
#include "spdlog/spdlog.h"
#include "Core.hpp"

using namespace Network::Switches;

Core::Core(const std::size_t portAmount)
: ISwitch(nextID++, portAmount)
{
    spdlog::trace("Created core switch with ID #{}", m_ID);

    // Initialize barrier requests
    for(std::size_t compNodeIdx = 0; compNodeIdx < Utilities::deriveComputingNodeAmount(m_portAmount); ++compNodeIdx){
        m_barrierRequestFlags.insert({compNodeIdx, false});
    }

    // Initialize reduce requests
    for(std::size_t portIdx = 0; portIdx < m_portAmount; ++portIdx){
        m_reduceStates.flags.insert({portIdx, false});
    }
}

bool Core::tick()
{
    // Advance all ports
    for(auto &port : m_ports) {
        port.tick();
    }

    // Check all ports for incoming messages
    static const auto compNodePerPort = std::size_t(std::pow(m_portAmount / 2, 2));
    for(size_t portIdx = 0; portIdx < m_ports.size(); ++portIdx) {
        auto &sourcePort = m_ports.at(portIdx);

        if(!sourcePort.hasIncoming()) {
            continue;
        }

        auto anyMsg = sourcePort.popIncoming();

        if(anyMsg->type() == typeid(Network::Message)) {
            const auto &msg = std::any_cast<const Network::Message&>(*anyMsg);

            spdlog::trace("Core Switch({}): Message received from sourcePort #{} destined to computing node #{}.", m_ID, portIdx, msg.m_destinationID);

            const auto targetPortIdx = msg.m_destinationID / compNodePerPort;
            spdlog::trace("Core Switch({}): Re-directing to port #{}..", m_ID, targetPortIdx);

            m_ports.at(targetPortIdx).pushOutgoing(std::move(anyMsg));

            if(portIdx == targetPortIdx) {
                spdlog::warn("Core Switch({}): Target and source ports are the same({})!", m_ID, portIdx);
            }
        }
        else if(anyMsg->type() == typeid(Network::BroadcastMessage)) {
            spdlog::trace("Core Switch({}): Broadcast message received from port #{}", m_ID, portIdx);

            // Re-direct to all other down-ports
            for(auto &port : m_ports) {

                if(sourcePort == port) {
                    continue;
                }

                port.pushOutgoing(std::make_unique<std::any>(*anyMsg));
            }
        }
        else if(anyMsg->type() == typeid(Network::BarrierRequest)) {
            // Process message
            {
                const auto &msg = std::any_cast<const Network::BarrierRequest&>(*anyMsg);

                if(auto &barrierRequest = m_barrierRequestFlags.at(msg.m_sourceID); barrierRequest) {
                    spdlog::warn("Core Switch({}): Computing node #{} already sent a barrier request!", m_ID, msg.m_sourceID);
                }
                else {
                    barrierRequest = true;
                }
            }

            // Check for barrier release
            {
                if(std::all_of(m_barrierRequestFlags.cbegin(), m_barrierRequestFlags.cend(), [](const auto& entry) { return entry.second; })) {
                    // Send barrier release to all ports
                    for(auto &port: m_ports) {
                        port.pushOutgoing(std::make_unique<std::any>(Network::BarrierRelease()));
                    }

                    // Reset all requests
                    for(auto &elem: m_barrierRequestFlags) {
                        elem.second = false;
                    }
                }
            }
        }
        else if(anyMsg->type() == typeid(Network::Reduce)) {
            // Process message
            {
                const auto &msg = std::any_cast<const Network::Reduce&>(*anyMsg);

                spdlog::trace("Core Switch({}): Received reduce message destined to computing node #{}.", m_ID, msg.m_destinationID);

                // Check if this is the first reduce message
                if(std::all_of(m_reduceStates.flags.cbegin(), m_reduceStates.flags.cend(), [](const auto& entry) { return !entry.second; })) {
                    m_reduceStates.destinationID = msg.m_destinationID;
                    m_reduceStates.opType        = msg.m_opType;
                    m_reduceStates.value         = msg.m_data;
                    m_reduceStates.flags.at(portIdx) = true;
                }
                else {
                    if(m_reduceStates.flags.at(portIdx)) {
                        spdlog::critical("Core Switch({}): Received multiple reduce messages from port #{}!", m_ID, portIdx);

                        throw std::runtime_error("Core Switch: Received multiple reduce messages!");
                    }

                    if(msg.m_opType != m_reduceStates.opType) {
                        spdlog::critical("Core Switch({}): Received reduce messages with different operation types from port #{}!", m_ID, portIdx);

                        throw std::runtime_error("Core Switch: Operation types doesn't match in reduce messages!");
                    }

                    m_reduceStates.flags.at(portIdx) = true;

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
                            spdlog::critical("Core Switch({}): Received reduce messages with unknown operation type from port #{}!", m_ID, portIdx);

                            throw std::runtime_error("Core Switch: Unknown operation type in reduce messages!");
                        }
                    }
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

                    auto msg = Network::Reduce(m_reduceStates.destinationID, m_reduceStates.opType);
                    msg.m_data = m_reduceStates.value;

                    m_ports.at(targetPortIdx).pushOutgoing(std::make_unique<std::any>(msg));

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
        else {
            spdlog::error("Core Switch({}): Cannot determine the type of received message!", m_ID);
            spdlog::debug("Type name was {}", anyMsg->type().name());

            return false;
        }
    }

    return true;
}
