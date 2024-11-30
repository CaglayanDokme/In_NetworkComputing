/**
 * @file    MPI.hpp
 * @brief   Implementation of the MPI class which stands for the computing and message passing interface of the network.
 * @author  Caglayan Dokme (caglayan.dokme@plan.space)
 * @date    March 17, 2024
 */

#include "MPI.hpp"

#include "Network/Switches/ISwitch.hpp"
#include "spdlog/spdlog.h"
#include "Constants.hpp"
#include <map>

using namespace Network;

MPI::MPI(const size_t ID)
: m_ID(ID)
{
    spdlog::trace("MPI({}): Created", m_ID);
}

void MPI::tick()
{
    // Advance port
    m_port.tick();

    if(m_ID == (Network::Constants::deriveComputingNodeAmount() - 1)) {
        ++currentTick;
    }

    if(!m_port.hasIncoming()) {
        return;
    }

    auto anyMsg = m_port.popIncoming();
    spdlog::trace("MPI({}): Received message type {}", m_ID, anyMsg->typeToString());

    ++m_statistics.total.received;

    if(anyMsg->m_destinationID.has_value()) {
        if(anyMsg->m_destinationID.value() != m_ID) {
            spdlog::critical("MPI({}): Received {} message for another destination({})!", m_ID, anyMsg->typeToString(), anyMsg->m_destinationID.value());

            throw std::logic_error("MPI: Invalid destination ID!");
        }
    }

    if(anyMsg->m_sourceID.has_value()) {
        if(anyMsg->m_sourceID.value() == m_ID) {
            spdlog::critical("MPI({}): Received message from itself!", m_ID);

            throw std::logic_error("MPI: Cannot receive message from itself!");
        }
    }

    switch(anyMsg->type()) {
        case Messages::e_Type::Acknowledge: {
            auto pMsg = std::move(std::unique_ptr<Messages::Acknowledge>(static_cast<Messages::Acknowledge *>(anyMsg.release())));

            spdlog::trace("MPI({}): Enqueueing {} acknowledgement from node #{}", m_ID, Messages::toString(pMsg->m_ackType), pMsg->m_sourceID.value());

            {
                std::lock_guard lock(m_acknowledge.mutex);
                m_acknowledge.messages.push_back(std::move(pMsg));
            }

            m_acknowledge.notifier.notify_all();

            ++m_statistics.acknowledge.received;

            break;
        }
        case Messages::e_Type::DirectMessage: {
            auto pMsg = std::move(std::unique_ptr<Messages::DirectMessage>(static_cast<Messages::DirectMessage *>(anyMsg.release())));

            spdlog::trace("MPI({}): Enqueueing direct message from node #{}", m_ID, pMsg->m_sourceID.value());

            {
                std::lock_guard lock(m_directReceive.mutex);
                m_directReceive.messages.push_back(std::move(pMsg));
            }

            m_directReceive.notifier.notify_all();

            ++m_statistics.directMsg.received;

            break;
        }
        case Messages::e_Type::BroadcastMessage: {
            auto pMsg = std::move(std::unique_ptr<Messages::BroadcastMessage>(static_cast<Messages::BroadcastMessage *>(anyMsg.release())));

            spdlog::trace("MPI({}): Enqueueing {} message from node #{}", m_ID, pMsg->typeToString(), pMsg->m_sourceID.value());

            {
                std::lock_guard lock(m_broadcastReceive.mutex);
                m_broadcastReceive.messages.push_back(std::move(pMsg));
            }

            m_broadcastReceive.notifier.notify_all();

            ++m_statistics.broadcast.received;

            break;
        }
        case Messages::e_Type::BarrierRequest: {
            auto pMsg = std::move(std::unique_ptr<Messages::BarrierRequest>(static_cast<Messages::BarrierRequest *>(anyMsg.release())));

            spdlog::trace("MPI({}): Enqueueing barrier request from node #{}", m_ID, pMsg->m_sourceID.value());

            if(Network::Switches::isNetworkComputingEnabled()) {
                spdlog::critical("MPI({}): Received barrier request in network computing mode!", m_ID);

                throw std::logic_error("MPI: Barrier request in network computing mode!");
            }

            {
                std::lock_guard lock(m_barrierRequest.mutex);
                m_barrierRequest.messages.push_back(std::move(pMsg));
            }

            m_barrierRequest.notifier.notify_all();

            ++m_statistics.barrier.received;

            break;
        }
        case Messages::e_Type::BarrierRelease: {
            auto pMsg = std::move(std::unique_ptr<Messages::BarrierRelease>(static_cast<Messages::BarrierRelease *>(anyMsg.release())));

            if(!Network::Switches::isNetworkComputingEnabled()) {
                spdlog::trace("MPI({}): Enqueueing barrier release from node #{}", m_ID, pMsg->m_sourceID.value());
            }
            else {
                spdlog::trace("MPI({}): Enqueueing barrier release..", m_ID);
            }

            {
                std::lock_guard lock(m_barrierRelease.mutex);
                m_barrierRelease.messages.push_back(std::move(pMsg));
            }

            m_barrierRelease.notifier.notify_all();

            ++m_statistics.barrier.received;

            break;
        }
        case Messages::e_Type::Reduce: {
            auto pMsg = std::move(std::unique_ptr<Messages::Reduce>(static_cast<Messages::Reduce *>(anyMsg.release())));

            spdlog::trace("MPI({}): Enqueueing {} message..", m_ID, pMsg->typeToString());

            {
                std::lock_guard lock(m_reduce.mutex);
                m_reduce.messages.push_back(std::move(pMsg));
            }

            m_reduce.notifier.notify_all();

            ++m_statistics.reduce.received;

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

            ++m_statistics.reduceAll.received;

            break;
        }
        case Messages::e_Type::Scatter: {
            auto pMsg = std::move(std::unique_ptr<Messages::Scatter>(static_cast<Messages::Scatter *>(anyMsg.release())));

            spdlog::trace("MPI({}): Enqueueing {} message from node #{}", m_ID, pMsg->typeToString(), pMsg->m_sourceID.value());

            {
                std::lock_guard lock(m_scatter.mutex);
                m_scatter.messages.push_back(std::move(pMsg));
            }

            m_scatter.notifier.notify_all();

            ++m_statistics.scatter.received;

            break;
        }
        case Messages::e_Type::Gather: {
            auto pMsg = std::move(std::unique_ptr<Messages::Gather>(static_cast<Messages::Gather *>(anyMsg.release())));

            spdlog::trace("MPI({}): Enqueueing {} message..", m_ID, pMsg->typeToString());

            {
                std::lock_guard lock(m_gather.mutex);
                m_gather.messages.push_back(std::move(pMsg));
            }

            m_gather.notifier.notify_all();

            ++m_statistics.gather.received;

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

            ++m_statistics.allGather.received;

            break;
        }
        default:  {
            spdlog::critical("MPI({}): Received unknown message type!", m_ID);

            ++m_statistics.unknown.received;

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

    m_statistics.directMsg.lastStart_tick = currentTick;

    // Send direct message
    {
        // Create a message
        auto msg = std::make_unique<Messages::DirectMessage>(m_ID, destinationID);

        msg->m_data = data;

        // Push the message to the port
        send(std::move(msg));
    }

    // Wait for the acknowledgement
    do {
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

                break;
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

            break;
        }
    } while(false);

    m_statistics.directMsg.lastEnd_tick = currentTick;
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

    m_statistics.directMsg.lastStart_tick = currentTick;

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
        send(std::move(msg));

        spdlog::trace("MPI({}): Sent {} acknowledgement to {}", m_ID, Messages::toString(Messages::e_Type::DirectMessage), sourceID);
    }

    m_statistics.directMsg.lastEnd_tick = currentTick;
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
    m_statistics.broadcast.lastStart_tick = currentTick;

    if(m_ID == sourceID) {
        spdlog::trace("MPI({}): Broadcasting..", m_ID);

        if(data.empty()) {
            spdlog::critical("MPI({}): Cannot send an empty message!", m_ID);

            throw std::invalid_argument("MPI cannot send empty message!");
        }

        static const auto compNodeAmount = Network::Constants::deriveComputingNodeAmount();

        // Broadcast message
        if(Network::Switches::isNetworkComputingEnabled()) {
            auto msg = std::make_unique<Messages::BroadcastMessage>(m_ID);
            msg->m_data = data;

            send(std::move(msg));
        }
        else {
            // Send the broadcast message to all computing nodes
            // TODO Can be optimized similar to the barrier
            for(size_t m_targetID = 0; m_targetID < compNodeAmount; ++m_targetID) {
                if(m_targetID == m_ID) {
                    continue;
                }

                auto msg = std::make_unique<Messages::BroadcastMessage>(m_ID, m_targetID);
                msg->m_data = data;

                send(std::move(msg));
            }
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
        const auto bAlreadyReceived = [&]() -> bool {
            auto msgIterator = std::find_if(m_broadcastReceive.messages.begin(), m_broadcastReceive.messages.end(), [sourceID](auto &&msg) {
                return (msg->m_sourceID.value() == sourceID);
            });

            if(msgIterator != m_broadcastReceive.messages.cend()) {
                auto &msg = *msgIterator->get();

                spdlog::trace("MPI({}): Received {} from message..", m_ID, msg.typeToString());

                data = std::move(msg.m_data);
                m_broadcastReceive.messages.erase(msgIterator);

                return true;
            }

            return false;
        }();

        // Wait for broadcast message
        while(!bAlreadyReceived) {
            m_broadcastReceive.notifier.wait(lock);

            auto &msg = *m_broadcastReceive.messages.back();

            if(msg.m_sourceID.value() != sourceID) {
                spdlog::warn("MPI({}): Received {} message from another source({}), expected node #{}", m_ID, msg.typeToString(), msg.m_sourceID.value(), sourceID);

                continue;
            }

            spdlog::trace("MPI({}): Received {} from node #{}", m_ID, msg.typeToString(), sourceID);

            data = std::move(msg.m_data);
            m_broadcastReceive.messages.pop_back();

            break;
        }
    }

    m_statistics.broadcast.lastEnd_tick = currentTick;
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
    m_statistics.barrier.lastStart_tick = currentTick;

    spdlog::trace("MPI({}): Barrier", m_ID);

    if(Network::Switches::isNetworkComputingEnabled()) {
        std::unique_lock lock(m_barrierRelease.mutex);

        // Send barrier request message
        {
            auto msg = std::make_unique<Messages::BarrierRequest>(m_ID);

            send(std::move(msg));
        }

        // Wait for the barrier to be released
        m_barrierRelease.notifier.wait(lock, [&]() { return !m_barrierRelease.messages.empty(); });
        m_barrierRelease.messages.pop_back();
    }
    else {
        static const size_t compNodeAmount    = Network::Constants::deriveComputingNodeAmount();
        static const size_t compNodePerColumn = Network::Constants::getPortPerSwitch() / 2;
        static const size_t compNodePerGroup  = compNodePerColumn * compNodePerColumn;
        static const size_t compNodePerHalf   = compNodeAmount / 2;
        static const size_t groupAmount       = compNodeAmount / compNodePerGroup;
        static const size_t columnPerGroup    = compNodePerGroup / compNodePerColumn;

        // Collect barrier requests
        {
            auto consumeRequests = [&](const std::vector<size_t> &sourceList) -> size_t {
                std::map<size_t, bool> bRequestMap;

                for(const auto &sourceID : sourceList) {
                    bRequestMap.insert({sourceID, false});
                }

                std::unique_lock reqLock(m_barrierRequest.mutex);

                std::erase_if(m_barrierRequest.messages, [&](auto &&msg) {
                    auto pRequestPair = std::find_if(bRequestMap.begin(), bRequestMap.end(), [&](auto &&bRequested) {
                        return (msg->m_sourceID.value() == bRequested.first);
                    });

                    if(pRequestPair == bRequestMap.end()) {
                        spdlog::debug("MPI({}): Couldn't find a request pair for source {}", m_ID, msg->m_sourceID.value());

                        return false;
                    }

                    if(pRequestPair->second) {
                        spdlog::debug("MPI({}): Received duplicate barrier request from node #{}, omitting..", m_ID, msg->m_sourceID.value());

                        return false;
                    }

                    spdlog::trace("MPI({}): Received barrier request from node #{}", m_ID, msg->m_sourceID.value());
                    pRequestPair->second = true;

                    return true;
                });

                size_t omittedMsgAmount = m_barrierRequest.messages.size();
                if(omittedMsgAmount > 0) {
                    spdlog::debug("MPI({}): Omitted {} messages..", m_ID, omittedMsgAmount);
                }

                while(true) {
                    if(std::all_of(bRequestMap.cbegin(), bRequestMap.cend(), [](const auto &bRequested) { return bRequested.second; })) {
                        break;
                    }

                    if(m_barrierRequest.messages.size() <= omittedMsgAmount) {
                        m_barrierRequest.notifier.wait(reqLock, [&]() { return (m_barrierRequest.messages.size() > omittedMsgAmount); });
                    }

                    auto &msg = *m_barrierRequest.messages.at(omittedMsgAmount);
                    auto pRequestPair = bRequestMap.find(msg.m_sourceID.value());

                    if((bRequestMap.cend() == pRequestPair) || pRequestPair->second) {
                        spdlog::debug("MPI({}): Received barrier request from an unexpected source({})! {} message waiting, {} omitted", m_ID, msg.m_sourceID.value(), m_barrierRequest.messages.size(), omittedMsgAmount);

                        if(0 != omittedMsgAmount) {
                            std::string omittedSourceIDs;
                            for(size_t msgIdx = 0; msgIdx < omittedMsgAmount; ++msgIdx) {
                                omittedSourceIDs += std::to_string( m_barrierRequest.messages.at(msgIdx)->m_sourceID.value()) + " ";
                            }

                            spdlog::trace("MPI({}): Omitted source IDs: {}", m_ID, omittedSourceIDs);
                        }

                        ++omittedMsgAmount;

                        continue;
                    }

                    spdlog::trace("MPI({}): Received barrier request from node #{}", m_ID, msg.m_sourceID.value());
                    pRequestPair->second = true;

                    m_barrierRequest.messages.erase(m_barrierRequest.messages.begin() + omittedMsgAmount);
                }

                return omittedMsgAmount;
            };

            if(m_ID < compNodePerHalf) {
                // Wait for barrier request from the same in-half-offset node in the right half
                const auto expectedSourceID = m_ID + compNodePerHalf;

                if(const auto omittedMsgAmount = consumeRequests({expectedSourceID}); omittedMsgAmount > 0) {
                    spdlog::debug("MPI({}): Omitted {} messages..", m_ID, omittedMsgAmount);
                }
            }
            else {
                // Send barrier request to the same in-half-offset node in the left half
                const auto targetID = m_ID - compNodePerHalf;

                spdlog::trace("MPI({}): Sending barrier request to node #{}", m_ID, targetID);

                auto msg = std::make_unique<Messages::BarrierRequest>(m_ID, targetID);
                send(std::move(msg));
            }

            if(m_ID < compNodePerGroup) {
                // Wait for barrier request from the same in-group-offset node in the other groups of the left half
                std::vector<size_t> sourceList;

                for(size_t groupID = 1; groupID < (groupAmount / 2); ++groupID) {
                    const auto expectedSourceID = m_ID + (groupID * compNodePerGroup);

                    sourceList.push_back(expectedSourceID);
                }

                if(const auto omittedMsgAmount = consumeRequests(sourceList); omittedMsgAmount > 0) {
                    spdlog::debug("MPI({}): Omitted {} messages..", m_ID, omittedMsgAmount);
                }
            }
            else if((compNodePerGroup <= m_ID) && (m_ID < compNodePerHalf)) {
                // Send barrier request to the same in-group-offset node in the first group
                const auto targetID = m_ID % compNodePerGroup;

                spdlog::trace("MPI({}): Sending barrier request to node #{}", m_ID, targetID);

                auto msg = std::make_unique<Messages::BarrierRequest>(m_ID, targetID);
                send(std::move(msg));
            }
            else {
                // Nothing to do
            }

            if(m_ID < compNodePerColumn) {
                // Wait for barrier request from the same in-column-offset node in the other columns of the first group
                std::vector<size_t> sourceList;
                for(size_t columnID = 1; columnID < columnPerGroup; ++columnID) {
                    const auto expectedSourceID = m_ID + (columnID * compNodePerColumn);

                    sourceList.push_back(expectedSourceID);
                }

                if(const auto omittedMsgAmount = consumeRequests(sourceList); omittedMsgAmount > 0) {
                    spdlog::debug("MPI({}): Omitted {} messages..", m_ID, omittedMsgAmount);
                }
            }
            else if((compNodePerColumn <= m_ID) && (m_ID < compNodePerGroup)) {
                // Send barrier request to the same in-column-offset node in the first column
                const auto targetID = m_ID % compNodePerColumn;

                spdlog::trace("MPI({}): Sending barrier request to node #{}", m_ID, targetID);

                auto msg = std::make_unique<Messages::BarrierRequest>(m_ID, targetID);
                send(std::move(msg));
            }
            else {
                // Nothing to do
            }

            if(0 == m_ID) {
                // Wait for barrier request from the same column nodes
                std::vector<size_t> sourceList;
                for(size_t expectedSourceID = 1; expectedSourceID < compNodePerColumn; ++expectedSourceID) {
                    sourceList.push_back(expectedSourceID);
                }

                if(const auto omittedMsgAmount = consumeRequests(sourceList); omittedMsgAmount > 0) {
                    spdlog::debug("MPI({}): Omitted {} messages..", m_ID, omittedMsgAmount);
                }
            }
            else if(m_ID < compNodePerColumn) {
                // Send barrier request to the first node
                spdlog::trace("MPI({}): Sending barrier request to node #0", m_ID);

                auto msg = std::make_unique<Messages::BarrierRequest>(m_ID, 0);
                send(std::move(msg));
            }
            else {
                // Nothing to do
            }

            if(0 != m_barrierRequest.messages.size()) {
                spdlog::error("MPI({}): {} barrier requests are pending!", m_ID, m_barrierRequest.messages.size());
            }

            if(0 == m_ID) {
                spdlog::debug("MPI({}): All barrier requests are completed", m_ID);
            }
        }

        // Barrier requests are completed, now release the barrier
        {
            auto consumeRelease = [&](const size_t &sourceID) -> void {
                std::unique_lock relLock(m_barrierRelease.mutex);

                bool bReleased = false;

                auto pAlreadyReceivedRelease = std::find_if(m_barrierRelease.messages.begin(), m_barrierRelease.messages.end(), [&](auto &&msg) {
                    return (msg->m_sourceID.value() == sourceID);
                });

                if(m_barrierRelease.messages.end() != pAlreadyReceivedRelease) {
                    spdlog::trace("MPI({}): Received barrier release from node #{}", m_ID, sourceID);

                    bReleased = true;

                    m_barrierRelease.messages.erase(pAlreadyReceivedRelease);
                }

                if(bReleased) {
                    return;
                }

                size_t omittedMsgAmount = m_barrierRelease.messages.size();

                if(omittedMsgAmount > 0) {
                    spdlog::debug("MPI({}): Omitted {} messages..", m_ID, omittedMsgAmount);
                }

                while(!bReleased) {
                    if(m_barrierRelease.messages.size() <= omittedMsgAmount) {
                        m_barrierRelease.notifier.wait(relLock, [&]() { return (m_barrierRelease.messages.size() > omittedMsgAmount); });
                    }

                    auto &msg = *m_barrierRelease.messages.at(omittedMsgAmount);

                    if(msg.m_sourceID.value() != sourceID) {
                        spdlog::debug("MPI({}): Received barrier release from an unexpected source({})! {} message waiting, {} omitted", m_ID, msg.m_sourceID.value(), m_barrierRelease.messages.size(), omittedMsgAmount);

                        if(0 != omittedMsgAmount) {
                            std::string omittedSourceIDs;
                            for(size_t msgIdx = 0; msgIdx < omittedMsgAmount; ++msgIdx) {
                                omittedSourceIDs += std::to_string( m_barrierRelease.messages.at(msgIdx)->m_sourceID.value()) + " ";
                            }

                            spdlog::trace("MPI({}): Omitted source IDs: {}", m_ID, omittedSourceIDs);
                        }

                        ++omittedMsgAmount;

                        continue;
                    }

                    spdlog::trace("MPI({}): Received barrier release from node #{}", m_ID, msg.m_sourceID.value());
                    bReleased = true;

                    m_barrierRelease.messages.erase(m_barrierRelease.messages.begin() + omittedMsgAmount);
                }

                return;
            };

            if(0 == m_ID) {
                // Send barrier release to all nodes in the same column
                for(size_t m_targetID = 1; m_targetID < compNodePerColumn; ++m_targetID) {
                    spdlog::trace("MPI({}): Sending barrier release to node #{}", m_ID, m_targetID);

                    auto msg = std::make_unique<Messages::BarrierRelease>(m_ID, m_targetID);
                    send(std::move(msg));
                }
            }
            else if(m_ID < compNodePerColumn) {
                // Wait for barrier release from the first node
                consumeRelease(0);
            }
            else {
                // Nothing to do
            }

            if(m_ID < compNodePerColumn) {
                // Send barrier release to all same in-column-offset nodes in the same group
                for(size_t columnID = 1; columnID < columnPerGroup; ++columnID) {
                    const auto targetID = m_ID + (columnID * compNodePerColumn);

                    spdlog::trace("MPI({}): Sending barrier release to node #{}", m_ID, targetID);

                    auto msg = std::make_unique<Messages::BarrierRelease>(m_ID, targetID);
                    send(std::move(msg));
                }
            }
            else if(m_ID < compNodePerGroup) {
                // Wait for barrier release from the same in-column-offset node in the first column
                consumeRelease(m_ID % compNodePerColumn);
            }
            else {
                // Nothing to do
            }

            if(m_ID < compNodePerGroup) {
                // Send barrier release to all same in-group-offset nodes in the other groups of the left half
                static const auto groupPerHalf = compNodePerHalf / compNodePerGroup;

                for(size_t groupID = 1; groupID < groupPerHalf; ++groupID) {
                    const auto targetID = m_ID + (groupID * compNodePerGroup);
                    spdlog::trace("MPI({}): Sending barrier release to node #{}", m_ID, targetID);

                    auto msg = std::make_unique<Messages::BarrierRelease>(m_ID, targetID);

                    send(std::move(msg));
                }
            }
            else if(m_ID < compNodePerHalf) {
                // Wait for barrier release from the same in-group-offset node in the first group
                consumeRelease(m_ID % compNodePerGroup);
            }
            else {
                // Nothing to do
            }

            if(m_ID < compNodePerHalf) {
                // Send barrier release to the same in-half-offset node in the right half
                const auto targetID = m_ID + compNodePerHalf;
                spdlog::trace("MPI({}): Sending barrier release to node #{}", m_ID, targetID);

                auto msg = std::make_unique<Messages::BarrierRelease>(m_ID, targetID);

                send(std::move(msg));
            }
            else {
                // Wait for a barrier release from the same in-half-offset node in the left half
                consumeRelease(m_ID - compNodePerHalf);
            }
        }
    }

    spdlog::trace("MPI({}): Barrier released", m_ID);

    m_statistics.barrier.lastEnd_tick = currentTick;
}

void MPI::reduce(std::vector<float> &data, const ReduceOp operation, const size_t destinationID)
{
    m_statistics.reduce.lastStart_tick = currentTick;

    spdlog::trace("MPI({}): Reducing data at {}", m_ID, destinationID);

    if(data.empty()) {
        spdlog::critical("MPI({}): Cannot join reduce with empty data container!", m_ID);

        throw std::invalid_argument("MPI: Cannot join reduce with empty data container!");
    }

    if(m_ID == destinationID) {
        if(Network::Switches::isNetworkComputingEnabled()) {
            std::unique_lock lock(m_reduce.mutex);

            bool bAlreadyReceived = false;

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

                    bAlreadyReceived = true;
                }
            }

            // Wait for the message
            while(!bAlreadyReceived) {
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
            std::unique_lock lock(m_reduce.mutex);

            std::vector<bool> rxFlags(Network::Constants::deriveComputingNodeAmount(), false);
            rxFlags.at(m_ID) = true;

            // Process incoming messages
            while(!std::all_of(rxFlags.cbegin(), rxFlags.cend(), [](const auto &flag) { return (true == flag); })) {
                if(m_reduce.messages.empty()) {
                    m_reduce.notifier.wait(lock, [&]() { return !m_reduce.messages.empty(); });
                }

                for(const auto &msg : m_reduce.messages) {
                    if(msg->m_data.size() != data.size()) {
                        spdlog::critical("MPI({}): Received data size({}) doesn't match the expected size({})!", m_ID, msg->m_data.size(), data.size());

                        throw std::runtime_error("MPI: Received data size doesn't match the expected size!");
                    }

                    if(msg->m_opType != operation) {
                        spdlog::critical("MPI({}): Received data with invalid operation({})! Expected {}", m_ID, Messages::toString(msg->m_opType), Messages::toString(operation));

                        throw std::runtime_error("MPI: Received data with invalid operation!");
                    }

                    if(rxFlags.at(msg->m_sourceID.value())) {
                        spdlog::critical("MPI({}): Received duplicate data from node #{}!", m_ID, msg->m_sourceID.value());

                        throw std::runtime_error("MPI: Received duplicate data!");
                    }

                    std::transform( data.cbegin(),
                                    data.cend(),
                                    msg->m_data.cbegin(),
                                    data.begin(),
                                    [operation](const float &lhs, const float &rhs) {
                                        return Messages::reduce(lhs, rhs, operation);
                                    });

                    rxFlags.at(msg->m_sourceID.value()) = true;
                }

                m_reduce.messages.clear();
            }
        }
    }
    else {
        auto msg = std::make_unique<Messages::Reduce>(m_ID, destinationID, operation);
        msg->m_data = data;

        // Push the message to the port
        send(std::move(msg));
    }

    m_statistics.reduce.lastEnd_tick = currentTick;
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
    m_statistics.reduceAll.lastStart_tick = currentTick;

    spdlog::trace("MPI({}): Reducing data", m_ID);

    if(Network::Switches::isNetworkComputingEnabled()) {
        const auto dataSize = data.size();

        // Lock here and avoid checking the already queued messages because the operation is incomplete unless every node has sent their data
        std::unique_lock lock(m_reduceAll.mutex);

        {
            auto msg = std::make_unique<Messages::ReduceAll>(operation);
            msg->m_data = std::move(data);

            send(std::move(msg));
        }

        // Wait for the message
        while(true) {
            m_reduceAll.notifier.wait(lock);

            if(m_reduceAll.messages.size() > 1) {
                spdlog::warn("MPI({}): Mutiple reduce-all messages({}) are pending, communication might be corrupted!", m_ID, m_reduceAll.messages.size());
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
    else {
        // Determine the root computing node
        static const auto rootNode = std::rand() % Network::Constants::deriveComputingNodeAmount();

        if(m_ID == rootNode) {
            spdlog::trace("MPI({}): Determined as the root node for all-reduce", m_ID);
        }

        reduce(data, operation, rootNode);

        if(m_ID != rootNode) {
            data.clear();
        }

        broadcast(data, rootNode);
    }

    m_statistics.reduceAll.lastEnd_tick = currentTick;
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

void MPI::scatter(std::vector<float> &data, const size_t sourceID)
{
    m_statistics.scatter.lastStart_tick = currentTick;

    spdlog::trace("MPI({}): Scattering data from {}", m_ID, sourceID);

    if(m_ID == sourceID) {
        if(data.empty()) {
            spdlog::critical("MPI({}): Cannot scatter empty data!", m_ID);

            throw std::invalid_argument("MPI cannot scatter empty data!");
        }

        const auto compNodeAmount = Network::Constants::deriveComputingNodeAmount();

        if(0 != (data.size() % compNodeAmount)) {
            spdlog::critical("MPI({}): Data size({}) is not divisible by the computing node amount({})!", m_ID, data.size(), compNodeAmount);

            throw std::runtime_error("MPI: Data size is not divisible by the computing node amount!");
        }

        const auto chunkSize = data.size() / compNodeAmount;

        std::vector<float> localChunk;
        localChunk.assign(data.cbegin() + (m_ID * chunkSize), data.cbegin() + ((m_ID + 1) * chunkSize));

        if(Network::Switches::isNetworkComputingEnabled()) {
            auto msg = std::make_unique<Messages::Scatter>(m_ID);
            msg->m_data = std::move(data);
            send(std::move(msg));
        }
        else {
            // Send the scatter message to all computing nodes
            // TODO Can be optimized similar to the barrier
            for(size_t m_targetID = 0; m_targetID < compNodeAmount; ++m_targetID) {
                if(m_targetID == m_ID) {
                    continue;
                }

                auto msg = std::make_unique<Messages::Scatter>(m_ID, m_targetID);

                const auto pChunkBegin = data.cbegin() + (m_targetID * chunkSize);
                const auto pChunkEnd = pChunkBegin + chunkSize;

                msg->m_data.assign(pChunkBegin, pChunkEnd);

                send(std::move(msg));
            }
        }

        data = std::move(localChunk);
    }
    else {
        if(!data.empty()) {
            spdlog::critical("MPI({}): Cannot receive into a non-empty destination!", m_ID);
            spdlog::debug("MPI({}): Destination had {} elements", m_ID, data.size());

            throw std::invalid_argument("Receive destination must be empty!");
        }

        do {
            std::unique_lock lock(m_scatter.mutex);

            bool bAlreadyReceived = false;

            // Check the already queued messages
            if(!m_scatter.messages.empty()) {
                auto msgIterator = std::find_if(m_scatter.messages.begin(), m_scatter.messages.end(), [sourceID](auto &&msg) {
                    return (msg->m_sourceID.value() == sourceID);
                });

                if(msgIterator != m_scatter.messages.cend()) {
                    auto &msg = *msgIterator->get();
                    spdlog::trace("MPI({}): Received scatter message from computing node #{}", m_ID, msg.m_sourceID.value());

                    data = std::move(msg.m_data);
                    m_scatter.messages.erase(msgIterator);

                    break;
                }
            }

            size_t omittedMsgAmount = m_scatter.messages.size();
            if(omittedMsgAmount > 0) {
                spdlog::debug("MPI({}): Omitted {} messages for scatter..", m_ID, omittedMsgAmount);
            }

            // Wait for the message
            while(!bAlreadyReceived) {
                if(m_scatter.messages.size() <= omittedMsgAmount) {
                    m_scatter.notifier.wait(lock, [&]() { return (m_scatter.messages.size() > omittedMsgAmount); });
                }

                auto &msg = *m_scatter.messages.at(omittedMsgAmount);

                if(msg.m_sourceID.value() != sourceID) {
                    spdlog::warn("MPI({}): Received scatter message from another source({}), expected note #{}", m_ID, msg.m_sourceID.value(), sourceID);

                    if(0 != omittedMsgAmount) {
                        std::string omittedSourceIDs;
                        for(size_t msgIdx = 0; msgIdx < omittedMsgAmount; ++msgIdx) {
                            omittedSourceIDs += std::to_string( m_scatter.messages.at(msgIdx)->m_sourceID.value()) + " ";
                        }

                        spdlog::trace("MPI({}): Omitted source IDs: {}", m_ID, omittedSourceIDs);
                    }

                    continue;
                }

                spdlog::trace("MPI({}): Received scatter data from node #{}", m_ID, sourceID);

                data = std::move(msg.m_data);
                m_scatter.messages.erase(m_scatter.messages.begin() + omittedMsgAmount);

                break;
            }
        } while(false);
    }

    m_statistics.scatter.lastEnd_tick = currentTick;
}

void MPI::gather(std::vector<float> &data, const size_t destinationID)
{
    m_statistics.gather.lastStart_tick = currentTick;

    spdlog::trace("MPI({}): Gathering data at {}", m_ID, destinationID);

    if(data.empty()) {
        spdlog::critical("MPI({}): Empty data given to gather!", m_ID);

        throw std::invalid_argument("MPI: Empty data given to gather!");
    }

    if(m_ID == destinationID) {
        if(Network::Switches::isNetworkComputingEnabled()) {
            std::unique_lock lock(m_gather.mutex);

            bool bAlreadyReceived = false;

            // Check the already queued messages
            do {
                auto msgIterator = std::find_if(m_gather.messages.begin(), m_gather.messages.end(), [destinationID](auto &&msg) {
                    return (msg->m_destinationID.value() == destinationID);
                });

                if(msgIterator != m_gather.messages.cend()) {
                    spdlog::trace("MPI({}): Gathering data with received message..", m_ID);

                    auto &msg = *msgIterator->get();

                    if((msg.m_data.size() % (Network::Constants::deriveComputingNodeAmount() - 1)) != 0) {
                        spdlog::critical("MPI({}): Received data size({}) is not divisible by the computing node amount({})!", m_ID, msg.m_data.size(), Network::Constants::deriveComputingNodeAmount() - 1);

                        throw std::runtime_error("MPI: Received data size is not divisible by the computing node amount!");
                    }

                    const auto chunkSize = msg.m_data.size() / (Network::Constants::deriveComputingNodeAmount() - 1);
                    spdlog::trace("MPI({}): Detected gather chunk size is {}", m_ID, chunkSize);

                    if(data.size() != chunkSize) {
                        spdlog::critical("MPI({}): Expected data size({}) doesn't match the received chunk size({})!", m_ID, data.size(), chunkSize);

                        break;
                    }

                    msg.m_data.insert(msg.m_data.begin() + (m_ID * chunkSize), data.cbegin(), data.cend());
                    data = std::move(msg.m_data);
                    m_gather.messages.erase(msgIterator);

                    bAlreadyReceived = true;

                    break;
                }
            } while(false);

            // Wait for the message
            while(!bAlreadyReceived) {
                m_gather.notifier.wait(lock);

                auto &msg = *m_gather.messages.back();

                if(msg.m_destinationID.value() != destinationID) {
                    spdlog::critical("MPI({}): Received data for invalid destination({}), expected {}!", m_ID, msg.m_destinationID.value(), destinationID);

                    continue;
                }

                if((msg.m_data.size() % (Network::Constants::deriveComputingNodeAmount() - 1)) != 0) {
                    spdlog::critical("MPI({}): Received data size({}) is not divisible by the computing node amount({})!", m_ID, msg.m_data.size(), Network::Constants::deriveComputingNodeAmount() - 1);

                    throw std::runtime_error("MPI: Received data size is not divisible by the computing node amount!");
                }

                const auto chunkSize = msg.m_data.size() / (Network::Constants::deriveComputingNodeAmount() - 1);
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
            std::unique_lock lock(m_gather.mutex);

            std::vector<
                std::pair<
                    size_t,             // Source ID
                    std::vector<float>  // Data
                >
            > receivedData;

            // Check the already queued messages
            {
                spdlog::trace("Checking already queued gather messages..");

                for(auto &&msg : m_gather.messages) {
                    auto pAlreadyReceived = std::find_if(receivedData.cbegin(), receivedData.cend(), [&](auto &&alreadyReceived) {
                        return (alreadyReceived.first == msg->m_sourceID.value());
                    });

                    if(receivedData.cend() != pAlreadyReceived) {
                        spdlog::critical("MPI({}): Received duplicate gather message from node #{}", m_ID, msg->m_sourceID.value());

                        throw std::logic_error("MPI: Duplicate gather message!");
                    }

                    receivedData.emplace_back(msg->m_sourceID.value(), std::move(msg->m_data));
                    spdlog::trace("MPI({}): Received gather message from node #{}", m_ID, msg->m_sourceID.value());
                }
                m_gather.messages.clear();
            }

            // Wait for the remaining messages
            while(receivedData.size() < (Network::Constants::deriveComputingNodeAmount() - 1)) {
                spdlog::trace("Waiting for gather messages..");

                m_gather.notifier.wait(lock, [&]() { return !m_gather.messages.empty(); });

                for(auto &&msg : m_gather.messages) {
                    auto pAlreadyReceived = std::find_if(receivedData.cbegin(), receivedData.cend(), [&](auto &&alreadyReceived) {
                        return (alreadyReceived.first == msg->m_sourceID.value());
                    });

                    if(receivedData.cend() != pAlreadyReceived) {
                        spdlog::critical("MPI({}): Received duplicate gather message from node #{}", m_ID, msg->m_sourceID.value());

                        throw std::logic_error("MPI: Duplicate gather message!");
                    }

                    receivedData.emplace_back(msg->m_sourceID.value(), std::move(msg->m_data));
                    spdlog::trace("MPI({}): Received gather message from node #{}", m_ID, msg->m_sourceID.value());
                }

                m_gather.messages.clear();
            }

            // Insert own data before sorting
            receivedData.emplace_back(m_ID, std::move(data));

            // Sort the received data
            std::sort(receivedData.begin(), receivedData.end(), [](const auto &lhs, const auto &rhs) {
                return (lhs.first < rhs.first);
            });

            // Gather the data
            for(auto &&[sourceID, chunk] : receivedData) {
                data.insert(data.end(), chunk.cbegin(), chunk.cend());
            }
        }
    }
    else {
        auto msg = std::make_unique<Messages::Gather>(m_ID, destinationID);
        msg->m_data = data;

        send(std::move(msg));
    }

    m_statistics.gather.lastEnd_tick = currentTick;
}

void MPI::allGather(std::vector<float> &data)
{
    m_statistics.allGather.lastStart_tick = currentTick;

    spdlog::trace("MPI({}): All-gathering data", m_ID);

    if(data.empty()) {
        spdlog::critical("MPI({}): Empty data given to all-gather!", m_ID);

        throw std::invalid_argument("MPI: Empty data given to all-gather!");
    }

    if(Network::Switches::isNetworkComputingEnabled()) {
        // Lock here and avoid checking the already queued messages because the operation is incomplete unless every node has sent their data
        std::unique_lock lock(m_allGather.mutex);

        const auto expectedSize = data.size() * Network::Constants::deriveComputingNodeAmount();

        // Send first
        {
            auto msg = std::make_unique<Messages::AllGather>();

            msg->m_data = std::move(data);

            send(std::move(msg));
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
    else {
        // Determine the root node
        static const auto rootNode = std::rand() % Network::Constants::deriveComputingNodeAmount();

        gather(data, rootNode);

        if(m_ID != rootNode) {
            data.clear();
        }

        broadcast(data, rootNode);
    }

    m_statistics.allGather.lastEnd_tick = currentTick;
}

void MPI::send(Network::Port::UniqueMsg msg)
{
    switch(msg->m_eType) {
        case Messages::e_Type::Acknowledge:
            ++m_statistics.acknowledge.sent;
            break;
        case Messages::e_Type::DirectMessage:
            ++m_statistics.directMsg.sent;
            break;
        case Messages::e_Type::BroadcastMessage:
            ++m_statistics.broadcast.sent;
            break;
        case Messages::e_Type::BarrierRequest:
            [[fallthrough]];
        case Messages::e_Type::BarrierRelease:
            ++m_statistics.barrier.sent;
            break;
        case Messages::e_Type::Reduce:
            ++m_statistics.reduce.sent;
            break;
        case Messages::e_Type::ReduceAll:
            ++m_statistics.reduceAll.sent;
            break;
        case Messages::e_Type::Scatter:
            ++m_statistics.scatter.sent;
            break;
        case Messages::e_Type::Gather:
            ++m_statistics.gather.sent;
            break;
        case Messages::e_Type::AllGather:
            ++m_statistics.allGather.sent;
            break;
        default:
            spdlog::warn("MPI({}): Unknown message type({}) sent!", m_ID, int(msg->m_eType));
            ++m_statistics.unknown.sent;

            break;
    }

    m_port.pushOutgoing(std::move(msg));
    ++m_statistics.total.sent;
}