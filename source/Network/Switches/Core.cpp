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
    for(std::size_t portIdx = 0; portIdx < m_portAmount; ++portIdx){
        m_reduceStates.flags.insert({portIdx, false});
    }

    // Initialize reduce-all requests
    for(std::size_t portIdx = 0; portIdx < m_portAmount; ++portIdx){
        m_reduceAllStates.flags.insert({portIdx, false});
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

        if(Messages::e_Type::Message == anyMsg->type()) {
            const auto &msg = *static_cast<const Messages::DirectMessage *>(anyMsg.get());

            spdlog::trace("Core Switch({}): Message received from sourcePort #{} destined to computing node #{}.", m_ID, portIdx, msg.m_destinationID);

            const auto targetPortIdx = msg.m_destinationID / compNodePerPort;
            spdlog::trace("Core Switch({}): Re-directing to port #{}..", m_ID, targetPortIdx);

            m_ports.at(targetPortIdx).pushOutgoing(std::move(anyMsg));

            if(portIdx == targetPortIdx) {
                spdlog::warn("Core Switch({}): Target and source ports are the same({})!", m_ID, portIdx);
            }
        }
        else if(Messages::e_Type::BroadcastMessage == anyMsg->type()) {
            spdlog::trace("Core Switch({}): Broadcast message received from port #{}", m_ID, portIdx);

            const auto &msg = *static_cast<const Messages::BroadcastMessage *>(anyMsg.get());

            // Re-direct to all other down-ports
            for(auto &port : m_ports) {
                if(sourcePort == port) {
                    continue;
                }

                auto uniqueMsg = std::make_unique<Network::Messages::BroadcastMessage>(msg);
                port.pushOutgoing(std::move(uniqueMsg));
            }
        }
        else if(Messages::e_Type::BarrierRequest == anyMsg->type()) {
            const auto &msg = *static_cast<const Messages::BarrierRequest *>(anyMsg.release());

            // Process message
            {
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
                        port.pushOutgoing(std::make_unique<Messages::BarrierRelease>());
                    }

                    // Reset all requests
                    for(auto &elem: m_barrierRequestFlags) {
                        elem.second = false;
                    }
                }
            }
        }
        else if(Messages::e_Type::Reduce == anyMsg->type()) {
            // Process message
            {
                const auto &msg = static_cast<const Messages::Reduce &>(*anyMsg.release());

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
                    m_reduceStates.value = Messages::reduce(m_reduceStates.value, msg.m_data, m_reduceStates.opType);
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
                    msg->m_data = m_reduceStates.value;

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
        else if(Messages::e_Type::ReduceAll == anyMsg->type()) {
            // Process message
            {
                const auto &msg = static_cast<const Messages::ReduceAll &>(*anyMsg.release());

                spdlog::trace("Core Switch({}): Received reduce-all message from port #{}.", m_ID, portIdx);

                // Check if this is the first reduce-all message
                if(!m_reduceAllStates.bOngoing) {
                    m_reduceAllStates.bOngoing = true;
                    m_reduceAllStates.opType = msg.m_opType;
                    m_reduceAllStates.value  = msg.m_data;
                    m_reduceAllStates.flags.at(portIdx) = true;
                }
                else {
                    if(m_reduceAllStates.flags.at(portIdx)) {
                        spdlog::critical("Core Switch({}): Received multiple reduce-all messages from port #{}!", m_ID, portIdx);

                        throw std::runtime_error("Core Switch: Received multiple reduce-all messages!");
                    }

                    if(msg.m_opType != m_reduceAllStates.opType) {
                        spdlog::critical("Core Switch({}): Received reduce-all messages with different operation types from port #{}!", m_ID, portIdx);

                        throw std::runtime_error("Core Switch: Operation types doesn't match in reduce-all messages!");
                    }

                    m_reduceAllStates.flags.at(portIdx) = true;
                    m_reduceAllStates.value = Messages::reduce(m_reduceAllStates.value, msg.m_data, m_reduceAllStates.opType);
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
        else {
            spdlog::error("Core Switch({}): Cannot determine the type of received message!", m_ID);
            spdlog::debug("Type name was {}", anyMsg->typeToString());

            return false;
        }
    }

    return true;
}
