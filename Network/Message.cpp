#include "Message.hpp"

using namespace Network;

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
