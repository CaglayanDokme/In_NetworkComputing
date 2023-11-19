#include "Network/Derivations.hpp"
#include "Network/Message.hpp"
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
            spdlog::trace("Core Switch({}): Re-directing to port #{}..", targetPortIdx);

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
        else {
            spdlog::error("Core Switch({}): Cannot determine the type of received message!", m_ID);
            spdlog::debug("Type name was {}", anyMsg->type().name());

            return false;
        }
    }

    return true;
}
