#include "Message.hpp"

using namespace Network::Messages;

Message::Message(const std::size_t sourceID, const std::size_t destinationID)
: m_sourceID(sourceID), m_destinationID(destinationID), m_data()
{
    // Nothing
}

BroadcastMessage::BroadcastMessage(const std::size_t sourceID)
: m_sourceID(sourceID)
{
    // Nothing
}

BarrierRequest::BarrierRequest(const std::size_t sourceID)
: m_sourceID(sourceID)
{
    // Nothing
}

Reduce::Reduce(const std::size_t destinationID, const OpType opType)
: m_destinationID(destinationID), m_opType(opType)
{
    // Nothing
}
