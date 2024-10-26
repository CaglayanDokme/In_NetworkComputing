#include "Message.hpp"
#include <map>

using namespace Network::Messages;

BaseMessage::BaseMessage(const e_Type eType, const Address &sourceID, const Address &destinationID)
: m_eType(eType), m_sourceID(sourceID), m_destinationID(destinationID)
{
    // Nothing
}

const std::string & BaseMessage::typeToString() const
{
    return toString(m_eType);
}

Acknowledge::Acknowledge(const size_t sourceID, const size_t destinationID, const e_Type ackType)
: BaseMessage(e_Type::Acknowledge, sourceID, destinationID), m_ackType(ackType)
{
    if(e_Type::Acknowledge == m_ackType) {
        throw std::invalid_argument("Acknowledge type cannot be Acknowledge");
    }
}

DirectMessage::DirectMessage(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::DirectMessage, sourceID, destinationID), m_data()
{
    // Nothing
}

BroadcastMessage::BroadcastMessage(const size_t sourceID)
: BaseMessage(e_Type::BroadcastMessage, sourceID)
{
    // Nothing
}

BroadcastMessage::BroadcastMessage(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::BroadcastMessage, sourceID, destinationID)
{
    // Nothing
}

BarrierRequest::BarrierRequest()
: BaseMessage(e_Type::BarrierRequest)
{
    // Nothing
}

BarrierRequest::BarrierRequest(const size_t sourceID)
: BaseMessage(e_Type::BarrierRequest, sourceID)
{
    // Nothing
}

BarrierRequest::BarrierRequest(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::BarrierRequest, sourceID, destinationID)
{
    // Nothing
}

BarrierRelease::BarrierRelease()
: BaseMessage(e_Type::BarrierRelease)
{
    // Nothing
}

BarrierRelease::BarrierRelease(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::BarrierRelease, sourceID, destinationID)
{
    // Nothing
}

Reduce::Reduce(const size_t destinationID, const OpType opType)
: BaseMessage(e_Type::Reduce, std::nullopt, destinationID), m_opType(opType)
{
    // Nothing
}

Reduce::Reduce(const size_t sourceID, const size_t destinationID, const OpType opType)
: BaseMessage(e_Type::Reduce, sourceID, destinationID), m_opType(opType)
{
    // Nothing
}

ReduceAll::ReduceAll(const OpType opType)
: BaseMessage(e_Type::ReduceAll), m_opType(opType)
{
    // Nothing
}

ReduceAll::ReduceAll(const size_t sourceID, const size_t destinationID, const OpType opType)
: BaseMessage(e_Type::ReduceAll, sourceID, destinationID), m_opType(opType)
{
    // Nothing
}

Scatter::Scatter(const size_t sourceID)
: BaseMessage(e_Type::Scatter, sourceID)
{
    // Nothing
}

Scatter::Scatter(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::Scatter, sourceID, destinationID)
{
    // Nothing
}

Gather::Gather(const size_t destinationID)
: BaseMessage(e_Type::Gather, std::nullopt, destinationID)
{
    // Nothing
}

Gather::Gather(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::Gather, sourceID, destinationID)
{
    // Nothing
}

AllGather::AllGather()
: BaseMessage(e_Type::AllGather)
{
    // Nothing
}

AllGather::AllGather(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::AllGather, sourceID, destinationID)
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
