#include "Port.hpp"
#include "Message.hpp"
#include <algorithm>
#include "spdlog/spdlog.h"

using namespace Network;

namespace PortDelays {
    static constexpr std::size_t incomingMsg = 5;
    static constexpr std::size_t outgoingMsg = 3;
}

bool Port::operator==(const Port &port) const
{
    return (this == &port);
}

Port::st_Msg::st_Msg(UniqueMsg data, const size_t &delay)
: data(std::move(data)), remaining(delay)
{
    // Nothing
}

void Port::pushIncoming(UniqueMsg msg)
{
    m_incoming.emplace_back(std::move(msg), PortDelays::incomingMsg);
}

void Port::pushOutgoing(UniqueMsg msg)
{
    m_outgoing.emplace_back(std::move(msg), PortDelays::outgoingMsg);
}

void Port::tick()
{
    // Transfer outgoing messages to remote port if required
    {
        if(!m_outgoing.empty()) {
            if(0 == m_outgoing.front().remaining) {
                if(m_pRemotePort) {
                    m_pRemotePort->pushIncoming(std::move(m_outgoing.front().data));
                    m_outgoing.pop_front();
                }
                else {
                    spdlog::error("Cannot transfer message to remote port!");
                }
            }
        }
    }

    // Adjust remaining counters
    {
        auto decrementTick = [&](st_Msg &element)
        {
            if(element.remaining > 0) {
                --element.remaining;
            }
        };

        std::for_each(m_incoming.begin(), m_incoming.end(), decrementTick);
        std::for_each(m_outgoing.begin(), m_outgoing.end(), decrementTick);
    }
}

bool Port::connect(Port &remotePort)
{
    if(m_pRemotePort || remotePort.m_pRemotePort) {
        spdlog::error("Already have a remote port!");

        return false;
    }

    m_pRemotePort = &remotePort;
    remotePort.m_pRemotePort = this;

    return true;
}

bool Port::isConnected() const
{
    return (m_pRemotePort && (this == m_pRemotePort->m_pRemotePort));
}

bool Port::hasIncoming() const
{
    if(m_incoming.empty()) {
        return false;
    }

    return (0 == m_incoming.front().remaining);
}

Port::UniqueMsg Port::popIncoming()
{
    if(!hasIncoming()) {
        spdlog::error("No incoming message, potential undefined behaviour!");

        return nullptr;
    }

    auto msg = std::move(m_incoming.front().data);
    m_incoming.pop_front();

    return msg;
}

std::size_t Port::outgoingAmount() const
{
    return m_outgoing.size();
}
