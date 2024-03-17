#include "Message.hpp"

using namespace Network::Messages;

std::string Network::Messages::toString(const e_Type eType)
{
    switch(eType) {
        case e_Type::Message:          return "Message";
        case e_Type::BroadcastMessage: return "BroadcastMessage";
        case e_Type::BarrierRequest:   return "BarrierRequest";
        case e_Type::BarrierRelease:   return "BarrierRelease";
        case e_Type::Reduce:           return "Reduce";
        case e_Type::ReduceAll:        return "ReduceAll";

        default:
            return "Unknown";
    }
}

BaseMessage::BaseMessage(const e_Type eType)
: m_eType(eType)
{
    // Nothing
}

std::string BaseMessage::typeToString() const
{
    return toString(m_eType);
}

DirectMessage::DirectMessage(const std::size_t sourceID, const std::size_t destinationID)
: BaseMessage(e_Type::Message), m_sourceID(sourceID), m_destinationID(destinationID), m_data()
{
    // Nothing
}

BroadcastMessage::BroadcastMessage(const std::size_t sourceID)
: BaseMessage(e_Type::BroadcastMessage), m_sourceID(sourceID)
{
    // Nothing
}

BarrierRequest::BarrierRequest(const std::size_t sourceID)
: BaseMessage(e_Type::BarrierRequest), m_sourceID(sourceID)
{
    // Nothing
}

BarrierRelease::BarrierRelease()
: BaseMessage(e_Type::BarrierRelease)
{
    // Nothing
}

Reduce::Reduce(const std::size_t destinationID, const OpType opType)
: BaseMessage(e_Type::Reduce), m_destinationID(destinationID), m_opType(opType)
{
    // Nothing
}

ReduceAll::ReduceAll(const OpType opType)
: BaseMessage(e_Type::ReduceAll), m_opType(opType)
{
    // Nothing
}
