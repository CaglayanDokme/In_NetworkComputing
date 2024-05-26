/**
 * @file    MPI.hpp
 * @brief   Implementation of the MPI class which stands for the computing and message passing interface of the network.
 * @author  Caglayan Dokme (caglayan.dokme@plan.space)
 * @date    March 17, 2024
 */

#include "MPI.hpp"

#include "spdlog/spdlog.h"
#include "Derivations.hpp"
#include <map>

using namespace Network;

MPI::MPI(const std::size_t ID)
: m_ID(ID)
{
    spdlog::trace("MPI({}): Created", m_ID);
}

void MPI::tick()
{
    // Advance port
    m_port.tick();

    if(!m_port.hasIncoming()) {
        return;
    }

    auto anyMsg = m_port.popIncoming();
    spdlog::trace("MPI({}): Received message type {}", m_ID, anyMsg->typeToString());

    switch(anyMsg->type()) {
        case Messages::e_Type::Acknowledge: {
            auto pMsg = std::move(std::unique_ptr<Messages::Acknowledge>(static_cast<Messages::Acknowledge *>(anyMsg.release())));

            if(pMsg->m_destinationID.value() != m_ID) {
                spdlog::critical("MPI({}): Received acknowledgement for another destination({})!", m_ID, pMsg->m_destinationID.value());

                throw std::logic_error("MPI: Invalid destination ID!");
            }

            spdlog::trace("MPI({}): Enqueueing {} acknowledgement from node #{}", m_ID, Messages::toString(pMsg->m_ackType), pMsg->m_sourceID.value());

            {
                std::lock_guard lock(m_acknowledge.mutex);
                m_acknowledge.messages.push_back(std::move(pMsg));
            }

            m_acknowledge.notifier.notify_all();

            break;
        }
        case Messages::e_Type::DirectMessage: {
            auto pMsg = std::move(std::unique_ptr<Messages::DirectMessage>(static_cast<Messages::DirectMessage *>(anyMsg.release())));

            if(pMsg->m_destinationID.value() != m_ID) {
                spdlog::critical("MPI({}): Received direct message for another destination({})!", m_ID, pMsg->m_destinationID.value());

                throw std::logic_error("MPI: Invalid destination ID!");
            }

            spdlog::trace("MPI({}): Enqueueing direct message from node #{}", m_ID, pMsg->m_sourceID.value());

            {
                std::lock_guard lock(m_directReceive.mutex);
                m_directReceive.messages.push_back(std::move(pMsg));
            }

            m_directReceive.notifier.notify_all();

            break;
        }
        case Messages::e_Type::BroadcastMessage: {
            auto pMsg = std::move(std::unique_ptr<Messages::BroadcastMessage>(static_cast<Messages::BroadcastMessage *>(anyMsg.release())));

            if(pMsg->m_sourceID.value() == m_ID) {
                spdlog::critical("MPI({}): Received broadcast message from itself!", m_ID);

                throw std::logic_error("MPI: Cannot receive broadcast message from itself!");
            }

            spdlog::trace("MPI({}): Enqueueing {} message from node #{}", m_ID, pMsg->typeToString(), pMsg->m_sourceID.value());

            {
                std::lock_guard lock(m_broadcastReceive.mutex);
                m_broadcastReceive.messages.push_back(std::move(pMsg));
            }

            m_broadcastReceive.notifier.notify_all();

            break;
        }
        case Messages::e_Type::BarrierRelease: {
            m_barrier.notifier.notify_all();

            break;
        }
        case Messages::e_Type::Reduce: {
            auto pMsg = std::move(std::unique_ptr<Messages::Reduce>(static_cast<Messages::Reduce *>(anyMsg.release())));

            if(pMsg->m_destinationID.value() != m_ID) {
                spdlog::critical("MPI({}): Received {} message for another destination({})!", m_ID, pMsg->typeToString(), pMsg->m_destinationID.value());

                throw std::logic_error("MPI: Invalid destination ID!");
            }

            spdlog::trace("MPI({}): Enqueueing {} message..", m_ID, pMsg->typeToString());

            {
                std::lock_guard lock(m_reduce.mutex);
                m_reduce.messages.push_back(std::move(pMsg));
            }

            m_reduce.notifier.notify_all();

            break;
        }
        case Messages::e_Type::ReduceAll: {
            auto pMsg = std::move(std::unique_ptr<Messages::ReduceAll>(static_cast<Messages::ReduceAll *>(anyMsg.release())));

            spdlog::trace("MPI({}): Enqueueing {} message..", m_ID, pMsg->typeToString());

            {
                std::lock_guard lock(m_reduceAll.mutex);
                m_reduceAll.messages.push_back(std::move(pMsg));
            }

            m_reduceAll.notifier.notify_all();

            break;
        }
        case Messages::e_Type::Scatter: {
            auto pMsg = std::move(std::unique_ptr<Messages::Scatter>(static_cast<Messages::Scatter *>(anyMsg.release())));

            if(pMsg->m_sourceID.value() == m_ID) {
                spdlog::critical("MPI({}): Received {} message from itself!", m_ID, pMsg->typeToString());

                throw std::logic_error("MPI: Cannot receive scatter message from itself!");
            }

            spdlog::trace("MPI({}): Enqueueing {} message from node #{}", m_ID, pMsg->typeToString(), pMsg->m_sourceID.value());

            {
                std::lock_guard lock(m_scatter.mutex);
                m_scatter.messages.push_back(std::move(pMsg));
            }

            m_scatter.notifier.notify_all();

            break;
        }
        case Messages::e_Type::Gather: {
            auto pMsg = std::move(std::unique_ptr<Messages::Gather>(static_cast<Messages::Gather *>(anyMsg.release())));

            if(pMsg->m_destinationID.value() != m_ID) {
                spdlog::critical("MPI({}): Received {} message for another destination({})!", m_ID, pMsg->typeToString(), pMsg->m_destinationID.value());

                throw std::logic_error("MPI: Invalid destination ID!");
            }

            spdlog::trace("MPI({}): Enqueueing {} message..", m_ID, pMsg->typeToString());

            {
                std::lock_guard lock(m_gather.mutex);
                m_gather.messages.push_back(std::move(pMsg));
            }

            m_gather.notifier.notify_all();

            break;
        }
        case Messages::e_Type::AllGather: {
            auto pMsg = std::move(std::unique_ptr<Messages::AllGather>(static_cast<Messages::AllGather *>(anyMsg.release())));

            spdlog::trace("MPI({}): Enqueueing {} message..", m_ID, pMsg->typeToString());

            {
                std::lock_guard lock(m_allGather.mutex);
                m_allGather.messages.push_back(std::move(pMsg));
            }

            m_allGather.notifier.notify_all();

            break;
        }
        default:  {
            spdlog::critical("MPI({}): Received unknown message type!", m_ID);

            throw std::runtime_error("MPI: Unknown message type!");
        }
    }
}

bool MPI::isReady() const
{
    return m_port.isConnected();
}

void MPI::send(const std::vector<float> &data, const size_t destinationID)
{
    spdlog::trace("MPI({}): Sending data to {}", m_ID, destinationID);

    if(data.empty()) {
        spdlog::critical("MPI({}): Cannot send an empty message!", m_ID);

        throw std::invalid_argument("MPI cannot send empty message!");
    }

    // Send direct message
    {
        // Create a message
        auto msg = std::make_unique<Messages::DirectMessage>(m_ID, destinationID);

        msg->m_data = data;

        // Push the message to the port
        m_port.pushOutgoing(std::move(msg));
    }

    // Wait for the acknowledgement
    {
        // Visit the acknowledgement queue
        {
            std::lock_guard lock(m_acknowledge.mutex);

            auto msgIterator = std::find_if(m_acknowledge.messages.begin(), m_acknowledge.messages.end(), [destinationID](auto &&msg) {
                return ((msg->m_sourceID.value() == destinationID) && (Messages::e_Type::DirectMessage == msg->m_ackType));
            });

            if(msgIterator != m_acknowledge.messages.cend()) {
                auto &msg = *msgIterator->get();

                spdlog::trace("MPI({}): Received {} acknowledgement from node #{}", m_ID, msg.typeToString(), destinationID);

                m_acknowledge.messages.erase(msgIterator);

                return;
            }
        }

        // Wait for the acknowledgement
        while(true) {
            std::unique_lock lock(m_acknowledge.mutex);
            m_acknowledge.notifier.wait(lock);

            const auto &msg = *m_acknowledge.messages.back();

            if(msg.m_sourceID.value() != destinationID) {
                spdlog::warn("MPI({}): Received acknowledgement from another source({}), expected note #{}", m_ID, msg.m_sourceID.value(), destinationID);

                continue;
            }

            if(msg.m_ackType != Messages::e_Type::DirectMessage) {
                spdlog::warn("MPI({}): Received acknowledgement of another type({})!", m_ID, msg.typeToString());

                continue;
            }

            spdlog::trace("MPI({}): Received {} acknowledgement from node #{}", m_ID, msg.typeToString(), destinationID);
            m_acknowledge.messages.pop_back();

            if(!m_acknowledge.messages.empty()) {
                spdlog::warn("MPI({}): More acknowledgements({}) are pending!", m_ID, m_acknowledge.messages.size());
            }

            return;
        }
    }

    throw std::runtime_error("MPI: Shall never reach here!");
}

void MPI::send(const float &data, const size_t destinationID)
{
    send(std::vector<float>{data}, destinationID);
}

void MPI::receive(std::vector<float> &data, const size_t sourceID)
{
    spdlog::trace("MPI({}): Receiving data from {}", m_ID, sourceID);

    if(!data.empty()) {
        spdlog::critical("MPI({}): Cannot receive into a non-empty destination!", m_ID);
        spdlog::debug("MPI({}): Destination had {} elements", m_ID, data.size());

        throw std::invalid_argument("Receive destination must be empty!");
    }

    if(m_ID == sourceID) {
        spdlog::critical("MPI({}): Cannot receive from itself!", m_ID);

        throw std::invalid_argument("MPI cannot receive from itself!");
    }

    // Wait for the message
    do {
        std::unique_lock lock(m_directReceive.mutex);

        // Visit the reception queue
        {
            auto msgIterator = std::find_if(m_directReceive.messages.begin(), m_directReceive.messages.end(), [sourceID](auto &&msg) {
                return (msg->m_sourceID.value() == sourceID);
            });

            if(msgIterator != m_directReceive.messages.cend()) {
                auto &msg = *msgIterator->get();

                spdlog::trace("MPI({}): Received {} from node #{}", m_ID, msg.typeToString(), sourceID);

                data = std::move(msg.m_data);

                m_directReceive.messages.erase(msgIterator);

                break;
            }
        }

        // Wait for the message
        while(true) {
            m_directReceive.notifier.wait(lock);

            auto &msg = *m_directReceive.messages.back();

            if(msg.m_sourceID.value() != sourceID) {
                spdlog::warn("MPI({}): Received message from another source({}), expected note #{}", m_ID, msg.m_sourceID.value(), sourceID);

                continue;
            }

            spdlog::trace("MPI({}): Received {} from node #{}", m_ID, msg.typeToString(), sourceID);

            data = std::move(msg.m_data);
            m_directReceive.messages.pop_back();

            break;
        }
    } while(false);

    // Send an acknowledgement
    {
        auto msg = std::make_unique<Messages::Acknowledge>(m_ID, sourceID, Messages::e_Type::DirectMessage);
        m_port.pushOutgoing(std::move(msg));

        spdlog::trace("MPI({}): Sent {} acknowledgement to {}", m_ID, Messages::toString(Messages::e_Type::DirectMessage), sourceID);
    }
}

void MPI::receive(float &data, const size_t sourceID)
{
    std::vector<float> temp;
    receive(temp, sourceID);

    if(1 != temp.size()) {
        spdlog::critical("MPI({}): Received data size({}) doesn't match the expected size(1)!", m_ID, temp.size());

        throw std::runtime_error("MPI: Received data size is not 1!");
    }

    data = temp.at(0);
}

void MPI::broadcast(std::vector<float> &data, const size_t sourceID)
{
    if(m_ID == sourceID) {
        spdlog::trace("MPI({}): Broadcasting..", m_ID);

        if(data.empty()) {
            spdlog::critical("MPI({}): Cannot send an empty message!", m_ID);

            throw std::invalid_argument("MPI cannot send empty message!");
        }

        // Broadcast message
        {
            auto msg = std::make_unique<Messages::BroadcastMessage>(m_ID);
            msg->m_data = data;

            m_port.pushOutgoing(std::move(msg));
        }

        // Wait for acknowledgements
        {
            std::unique_lock lock(m_acknowledge.mutex);

            const auto compNodeAmount = Network::Utilities::deriveComputingNodeAmount();
            std::vector<bool> acks(compNodeAmount, false);
            acks.at(m_ID) = true; // Skip the broadcaster

            {
                std::remove_if(m_acknowledge.messages.begin(), m_acknowledge.messages.end(), [&](auto &&msg) {
                    if(Messages::e_Type::BroadcastMessage != msg->m_ackType) {
                        return false;
                    }

                    if(acks.at(msg->m_sourceID.value())) {
                        spdlog::critical("MPI({}): Received duplicate acknowledgement from node #{}", m_ID, msg->m_sourceID.value());

                        throw std::logic_error("MPI: Duplicate acknowledgement!");
                    }

                    spdlog::trace("MPI({}): Received {} acknowledgement from node #{}", m_ID, msg->typeToString(), msg->m_sourceID.value());
                    acks.at(msg->m_sourceID.value()) = true;

                    return true;
                });
            }

            while(!std::all_of(acks.cbegin(), acks.cend(), [](const bool ack) { return ack; })) {
                spdlog::trace("MPI({}): Waiting for broadcast acknowledgements..", m_ID);
                m_acknowledge.notifier.wait(lock);

                const auto &msg = *m_acknowledge.messages.back();

                if(Messages::e_Type::BroadcastMessage != msg.m_ackType) {
                    spdlog::warn("MPI({}): While waiting for broadcast acknowledge, received acknowledgement of another type({})!", m_ID, msg.typeToString());

                    continue;
                }

                if(acks.at(msg.m_sourceID.value())) {
                    spdlog::critical("MPI({}): Received duplicate acknowledgement from node #{}", m_ID, msg.m_sourceID.value());

                    throw std::logic_error("MPI: Duplicate acknowledgement!");
                }

                spdlog::trace("MPI({}): Received {} acknowledgement from node #{}", m_ID, msg.typeToString(), msg.m_sourceID.value());
                acks.at(msg.m_sourceID.value()) = true;
            }

            spdlog::trace("MPI({}): Received all acknowledgements", m_ID);
        }
    }
    else {
        if(!data.empty()) {
            spdlog::critical("MPI({}): Cannot receive into a non-empty destination!", m_ID);
            spdlog::debug("MPI({}): Destination had {} elements", m_ID, data.size());

            throw std::invalid_argument("Receive destination must be empty!");
        }

        spdlog::trace("MPI({}): Receiving broadcast from {}", m_ID, sourceID);
        std::unique_lock lock(m_broadcastReceive.mutex);

        // Check the already queued messages
        {
            auto msgIterator = std::find_if(m_broadcastReceive.messages.begin(), m_broadcastReceive.messages.end(), [sourceID](auto &&msg) {
                return (msg->m_sourceID.value() == sourceID);
            });

            if(msgIterator != m_broadcastReceive.messages.cend()) {
                auto &msg = *msgIterator->get();

                spdlog::trace("MPI({}): Received {} from message..", m_ID, msg.typeToString());

                data = std::move(msg.m_data);
                m_broadcastReceive.messages.erase(msgIterator);

                return;
            }
        }

        // Wait for broadcast message
        while(true) {
            m_broadcastReceive.notifier.wait(lock);

            auto &msg = *m_broadcastReceive.messages.back();

            if(msg.m_sourceID.value() != sourceID) {
                spdlog::warn("MPI({}): Received {} message from another source({}), expected note #{}", m_ID, msg.typeToString(), msg.m_sourceID.value(), sourceID);

                continue;
            }

            spdlog::trace("MPI({}): Received {} from node #{}", m_ID, msg.typeToString(), sourceID);

            data = std::move(msg.m_data);
            m_broadcastReceive.messages.pop_back();
        }

        // Send an acknowledgement
        {
            auto msg = std::make_unique<Messages::Acknowledge>(m_ID, sourceID, Messages::e_Type::BroadcastMessage);
            m_port.pushOutgoing(std::move(msg));

            spdlog::trace("MPI({}): Sent broadcast acknowledgement to {}", m_ID, sourceID);
        }
    }
}

void MPI::broadcast(float &data, const size_t sourceID)
{
    if(sourceID == m_ID) {
        std::vector<float> temp;
        temp.push_back(data);

        broadcast(temp, sourceID);
    }
    else {
        std::vector<float> temp;
        broadcast(temp, sourceID);

        if(1 != temp.size()) {
            spdlog::critical("MPI({}): Received data size({}) doesn't match the expected size(1)!", m_ID, temp.size());

            throw std::runtime_error("MPI: Received data size is not 1!");
        }

        data = temp.at(0);
    }
}

void MPI::barrier()
{
    spdlog::trace("MPI({}): Barrier", m_ID);

    std::unique_lock lock(m_barrier.mutex);

    // Send barrier request message
    {
        auto msg = std::make_unique<Messages::BarrierRequest>(m_ID);

        m_port.pushOutgoing(std::move(msg));
    }

    // Wait for the barrier to be released
    m_barrier.notifier.wait(lock);

    spdlog::trace("MPI({}): Barrier released", m_ID);
}

void MPI::reduce(std::vector<float> &data, const ReduceOp operation, const size_t destinationID)
{
    spdlog::trace("MPI({}): Reducing data at {}", m_ID, destinationID);

    if(data.empty()) {
        spdlog::critical("MPI({}): Cannot join reduce with empty data container!", m_ID);

        throw std::invalid_argument("MPI: Cannot join reduce with empty data container!");
    }

    if(m_ID == destinationID) {
        std::unique_lock lock(m_reduce.mutex);

        // Check the already queued messages
        {
            auto msgIterator = std::find_if(m_reduce.messages.begin(), m_reduce.messages.end(), [&](auto &&msg) {
                if(msg->m_opType != operation) {
                    return false;
                }

                if(msg->m_data.size() != data.size()) {
                    spdlog::warn("MPI({}): Received data size({}) doesn't match the expected size({})!", m_ID, msg->m_data.size(), data.size());

                    return false;
                }

                return true;
            });

            if(msgIterator != m_reduce.messages.cend()) {
                spdlog::trace("MPI({}): Reducing data with received message..", m_ID);

                auto &msg = *msgIterator->get();

                std::transform( data.cbegin(),
                                data.cend(),
                                msg.m_data.cbegin(),
                                data.begin(),
                                [operation](const float &lhs, const float &rhs) {
                                    return Messages::reduce(lhs, rhs, operation);
                                });

                m_reduce.messages.erase(msgIterator);

                return;
            }
        }

        // Wait for the message
        while(true) {
            m_reduce.notifier.wait(lock);

            auto &msg = *m_reduce.messages.back();

            if(msg.m_opType != operation) {
                spdlog::warn("MPI({}): Received data with invalid operation({})! Expected {}", m_ID, Messages::toString(msg.m_opType), Messages::toString(operation));

                continue;
            }

            if(msg.m_data.size() != data.size()) {
                spdlog::warn("MPI({}): Received data size({}) doesn't match the expected size({})!", m_ID, msg.m_data.size(), data.size());

                continue;
            }

            spdlog::trace("MPI({}): Reducing data with received message..", m_ID);

            std::transform( data.cbegin(),
                            data.cend(),
                            msg.m_data.cbegin(),
                            data.begin(),
                            [operation](const float &lhs, const float &rhs) {
                                return Messages::reduce(lhs, rhs, operation);
                            });

            m_reduce.messages.pop_back();

            break;
        }
    }
    else {
        auto msg = std::make_unique<Messages::Reduce>(destinationID, operation);
        msg->m_data = data;

        // Push the message to the port
        m_port.pushOutgoing(std::move(msg));
    }
}

void MPI::reduce(float &data, const ReduceOp operation, const size_t destinationID)
{
    std::vector<float> temp;
    temp.push_back(data);

    reduce(temp, operation, destinationID);

    if(1 != temp.size()) {
        spdlog::critical("MPI({}): Received data size({}) doesn't match the expected size(1)!", m_ID, temp.size());

        throw std::runtime_error("MPI: Received data size is not 1!");
    }

    data = temp.at(0);
}

void MPI::reduceAll(std::vector<float> &data, const ReduceOp operation)
{
    spdlog::trace("MPI({}): Reducing data", m_ID);

    const auto dataSize = data.size();

    // Lock here and avoid checking the already queued messages because the operation is incomplete unless every node has sent their data
    std::unique_lock lock(m_reduceAll.mutex);

    {
        auto msg = std::make_unique<Messages::ReduceAll>(operation);
        msg->m_data = std::move(data);

        m_port.pushOutgoing(std::move(msg));
    }

    // Wait for the message
    while(true) {
        m_reduceAll.notifier.wait(lock);

        if(m_reduceAll.messages.size() > 1) {
            spdlog::warn("MPI({}): More reduce-all messages({}) are pending, communication might be corrupted!", m_ID, m_reduceAll.messages.size());
        }

        auto &msg = *m_reduceAll.messages.back();

        if(msg.m_opType != operation) {
            spdlog::warn("MPI({}): Received data with invalid reduce-all operation({})! Expected {}", m_ID, Messages::toString(msg.m_opType), Messages::toString(operation));

            continue;
        }

        if(msg.m_data.size() != dataSize) {
            spdlog::warn("MPI({}): Received data size({}) doesn't match the expected size({})!", m_ID, msg.m_data.size(), dataSize);

            continue;
        }

        data = std::move(msg.m_data);
        m_reduceAll.messages.pop_back();

        break;
    }
}

void MPI::reduceAll(float &data, const ReduceOp operation)
{
    std::vector<float> temp;
    temp.push_back(data);

    reduceAll(temp, operation);

    if(1 != temp.size()) {
        spdlog::critical("MPI({}): Received data size({}) doesn't match the expected size(1)!", m_ID, temp.size());

        throw std::runtime_error("MPI: Received data size is not 1!");
    }

    data = temp.at(0);
}

void MPI::scatter(std::vector<float> &data, const std::size_t sourceID)
{
    spdlog::trace("MPI({}): Scattering data from {}", m_ID, sourceID);

    if(m_ID == sourceID) {
        if(data.empty()) {
            spdlog::critical("MPI({}): Cannot scatter empty data!", m_ID);

            throw std::invalid_argument("MPI cannot scatter empty data!");
        }

        const auto compNodeAmount = Network::Utilities::deriveComputingNodeAmount();

        if(0 != (data.size() % compNodeAmount)) {
            spdlog::critical("MPI({}): Data size({}) is not divisible by the computing node amount({})!", m_ID, data.size(), compNodeAmount);

            throw std::runtime_error("MPI: Data size is not divisible by the computing node amount!");
        }

        const auto chunkSize = data.size() / compNodeAmount;

        std::vector<float> localChunk;
        localChunk.assign(data.cbegin() + (m_ID * chunkSize), data.cbegin() + ((m_ID + 1) * chunkSize));
        data.erase(data.cbegin() + (m_ID * chunkSize), data.cbegin() + ((m_ID + 1) * chunkSize));

        auto msg = std::make_unique<Messages::Scatter>(m_ID);
        msg->m_data = std::move(data);
        m_port.pushOutgoing(std::move(msg));

        data = std::move(localChunk);
    }
    else {
        if(!data.empty()) {
            spdlog::critical("MPI({}): Cannot receive into a non-empty destination!", m_ID);
            spdlog::debug("MPI({}): Destination had {} elements", m_ID, data.size());

            throw std::invalid_argument("Receive destination must be empty!");
        }

        std::unique_lock lock(m_scatter.mutex);

        // Check the already queued messages
        {
            auto msgIterator = std::find_if(m_scatter.messages.begin(), m_scatter.messages.end(), [sourceID](auto &&msg) {
                return (msg->m_sourceID.value() == sourceID);
            });

            if(msgIterator != m_scatter.messages.cend()) {
                spdlog::trace("MPI({}): Received data from message..", m_ID);

                auto &msg = *msgIterator->get();

                data = std::move(msg.m_data);
                m_scatter.messages.erase(msgIterator);

                return;
            }
        }

        // Wait for the message
        while(true) {
            m_scatter.notifier.wait(lock);

            auto &msg = *m_scatter.messages.back();

            if(msg.m_sourceID.value() != sourceID) {
                spdlog::warn("MPI({}): Received message from another source({}), expected note #{}", m_ID, msg.m_sourceID.value(), sourceID);

                continue;
            }

            spdlog::trace("MPI({}): Received scatter data from node #{}", m_ID, sourceID);

            data = std::move(msg.m_data);
            m_scatter.messages.pop_back();

            break;
        }
    }
}

void MPI::gather(std::vector<float> &data, const std::size_t destinationID)
{
    spdlog::trace("MPI({}): Gathering data at {}", m_ID, destinationID);

    if(data.empty()) {
        spdlog::critical("MPI({}): Empty data given to gather!", m_ID);

        throw std::invalid_argument("MPI: Empty data given to gather!");
    }

    if(m_ID == destinationID) {
        std::unique_lock lock(m_gather.mutex);

        // Check the already queued messages
        do {
            auto msgIterator = std::find_if(m_gather.messages.begin(), m_gather.messages.end(), [destinationID](auto &&msg) {
                return (msg->m_destinationID.value() == destinationID);
            });

            if(msgIterator != m_gather.messages.cend()) {
                spdlog::trace("MPI({}): Gathering data with received message..", m_ID);

                auto &msg = *msgIterator->get();

                if((msg.m_data.size() % (Network::Utilities::deriveComputingNodeAmount() - 1)) != 0) {
                    spdlog::critical("MPI({}): Received data size({}) is not divisible by the computing node amount({})!", m_ID, msg.m_data.size(), Network::Utilities::deriveComputingNodeAmount() - 1);

                    throw std::runtime_error("MPI: Received data size is not divisible by the computing node amount!");
                }

                const auto chunkSize = msg.m_data.size() / (Network::Utilities::deriveComputingNodeAmount() - 1);
                spdlog::trace("MPI({}): Detected gather chunk size is {}", m_ID, chunkSize);

                if(data.size() != chunkSize) {
                    spdlog::critical("MPI({}): Expected data size({}) doesn't match the received chunk size({})!", m_ID, data.size(), chunkSize);

                    break;
                }

                msg.m_data.insert(msg.m_data.begin() + (m_ID * chunkSize), data.cbegin(), data.cend());
                data = std::move(msg.m_data);
                m_gather.messages.erase(msgIterator);

                return;
            }
        } while(false);

        // Wait for the message
        while(true) {
            m_gather.notifier.wait(lock);

            auto &msg = *m_gather.messages.back();

            if(msg.m_destinationID.value() != destinationID) {
                spdlog::critical("MPI({}): Received data for invalid destination({}), expected {}!", m_ID, msg.m_destinationID.value(), destinationID);

                continue;
            }

            if((msg.m_data.size() % (Network::Utilities::deriveComputingNodeAmount() - 1)) != 0) {
                spdlog::critical("MPI({}): Received data size({}) is not divisible by the computing node amount({})!", m_ID, msg.m_data.size(), Network::Utilities::deriveComputingNodeAmount() - 1);

                throw std::runtime_error("MPI: Received data size is not divisible by the computing node amount!");
            }

            const auto chunkSize = msg.m_data.size() / (Network::Utilities::deriveComputingNodeAmount() - 1);
            spdlog::trace("MPI({}): Detected gather chunk size is {}", m_ID, chunkSize);

            if(data.size() != chunkSize) {
                spdlog::critical("MPI({}): Expected data size({}) doesn't match the received chunk size({})!", m_ID, data.size(), chunkSize);

                continue;
            }

            msg.m_data.insert(msg.m_data.begin() + (m_ID * chunkSize), data.cbegin(), data.cend());
            data = std::move(msg.m_data);
            m_gather.messages.pop_back();

            break;
        }
    }
    else {
        auto msg = std::make_unique<Messages::Gather>(destinationID);
        msg->m_data = data;

        m_port.pushOutgoing(std::move(msg));
    }
}

void MPI::allGather(std::vector<float> &data)
{
    spdlog::trace("MPI({}): All-gathering data", m_ID);

    if(data.empty()) {
        spdlog::critical("MPI({}): Empty data given to all-gather!", m_ID);

        throw std::invalid_argument("MPI: Empty data given to all-gather!");
    }

    const auto expectedSize = data.size() * Network::Utilities::deriveComputingNodeAmount();

    // Lock here and avoid checking the already queued messages because the operation is incomplete unless every node has sent their data
    std::unique_lock lock(m_allGather.mutex);

    // Send first
    {
        auto msg = std::make_unique<Messages::AllGather>();

        msg->m_data = std::move(data);

        m_port.pushOutgoing(std::move(msg));
    }

    // Wait for the message
    while(true) {
        m_allGather.notifier.wait(lock);

        auto &msg = *m_allGather.messages.back();

        if(msg.m_data.size() != expectedSize) {
            spdlog::warn("MPI({}): Received data size({}) doesn't match the expected size({})!", m_ID, msg.m_data.size(), expectedSize);

            continue;
        }

        spdlog::trace("MPI({}): All-gathering data with received message..", m_ID);

        data = std::move(msg.m_data);
        m_allGather.messages.pop_back();

        break;
    }
}
