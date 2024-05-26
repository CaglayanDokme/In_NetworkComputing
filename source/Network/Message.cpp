#include "Message.hpp"
#include <map>

using namespace Network::Messages;

BaseMessage::BaseMessage(const e_Type eType)
: m_eType(eType)
{
    // Nothing
}

const std::string & BaseMessage::typeToString() const
{
    return toString(m_eType);
}

Acknowledge::Acknowledge(const std::size_t sourceID, const std::size_t destinationID, const e_Type ackType)
: BaseMessage(e_Type::Acknowledge), m_sourceID(sourceID), m_destinationID(destinationID), m_ackType(ackType)
{
    if(e_Type::Acknowledge == m_ackType) {
        throw std::invalid_argument("Acknowledge type cannot be Acknowledge");
    }
}

DirectMessage::DirectMessage(const std::size_t sourceID, const std::size_t destinationID)
: BaseMessage(e_Type::DirectMessage), m_sourceID(sourceID), m_destinationID(destinationID), m_data()
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

Scatter::Scatter(const std::size_t sourceID)
: BaseMessage(e_Type::Scatter), m_sourceID(sourceID)
{
    // Nothing
}

Gather::Gather(const std::size_t destinationID)
: BaseMessage(e_Type::Gather), m_destinationID(destinationID)
{
    // Nothing
}

AllGather::AllGather()
: BaseMessage(e_Type::AllGather)
{
    // Nothing
}

const std::string &Network::Messages::toString(const e_Type eType)
{
    static const std::map<e_Type, std::string> strMap {
        {e_Type::Acknowledge,      "Acknowledge"},
        {e_Type::DirectMessage,    "DirectMessage"},
        {e_Type::BroadcastMessage, "BroadcastMessage"},
        {e_Type::BarrierRequest,   "BarrierRequest"},
        {e_Type::BarrierRelease,   "BarrierRelease"},
        {e_Type::Reduce,           "Reduce"},
        {e_Type::ReduceAll,        "ReduceAll"},
        {e_Type::Scatter,          "Scatter"},
        {e_Type::Gather,           "Gather"},
        {e_Type::AllGather,        "AllGather"},

        {e_Type::IS_Scatter,       "Inter-Switch Scatter"},
        {e_Type::IS_Gather,        "Inter-Switch Gather"},
        {e_Type::IS_AllGather,     "Inter-Switch AllGather"},
    };

    return strMap.at(eType);
}

const std::string &Network::Messages::toString(const ReduceOperation opType)
{
    static const std::map<ReduceOperation, std::string> strMap {
        {ReduceOperation::Sum,      "Sum"},
        {ReduceOperation::Multiply, "Multiply"},
        {ReduceOperation::Max,      "Max"},
        {ReduceOperation::Min,      "Min"},
    };

    return strMap.at(opType);
}
