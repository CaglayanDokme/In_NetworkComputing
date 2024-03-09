#include "Computer.hpp"
#include "spdlog/spdlog.h"
#include "Network/Message.hpp"

Computer::Computer()
: m_ID(nextID++)
{
    spdlog::trace("Created computing node with ID #{}", m_ID);

    if(m_ID >= computingNodeAmount) {
        spdlog::critical("Invalid ID({}) or total number of computing nodes({})!", m_ID, computingNodeAmount);

        if(0 == computingNodeAmount) {
            spdlog::warn("Total number of computing nodes wasn't set!");
        }

        throw std::invalid_argument("Invalid ID or total number of computing nodes!");
    }

    if(nextID == computingNodeAmount) {
        spdlog::trace("That was the last computing node to be spawned.");
    }
}

void Computer::setTotalAmount(const std::size_t totalAmount)
{
    if((0 != computingNodeAmount) && (totalAmount != computingNodeAmount)) {
        spdlog::critical("Computing node amount was set to({}) before!", computingNodeAmount);

        throw std::logic_error("Cannot modify computing node amount!");
    }

    computingNodeAmount = totalAmount;

    spdlog::debug("Total amount of computing nodes set to {}..", computingNodeAmount);
}

bool Computer::tick()
{
    // Advance port
    m_port.tick();

        if(m_port.hasIncoming()) {
        auto anyMsg = m_port.popIncoming();

        if(Network::Messages::e_Type::Message == anyMsg->type()) {
            const auto &msg = *static_cast<const Network::Messages::DirectMessage *>(anyMsg.release());

            spdlog::trace("Computer({}): Received message from node #{}", m_ID, msg.m_sourceID);

            if(msg.m_destinationID != m_ID) {
                spdlog::error("Computer({}): Message delivered to wrong computer! Actual destination was #{}", m_ID, msg.m_destinationID);
            }
        }
        else if(Network::Messages::e_Type::BroadcastMessage == anyMsg->type()) {
            const auto &msg = *static_cast<const Network::Messages::BroadcastMessage *>(anyMsg.release());

            spdlog::trace("Computer({}): Received broadcast message from node #{}", m_ID, msg.m_sourceID);
        }
        else if(Network::Messages::e_Type::BarrierRelease == anyMsg->type()) {
            spdlog::debug("Computer({}): Barrier release received!", m_ID);
        }
        else if(Network::Messages::e_Type::Reduce == anyMsg->type()) {
            const auto &msg = *static_cast<const Network::Messages::Reduce *>(anyMsg.release());

            spdlog::info("Computer({}): Received reduce message!", m_ID);
            spdlog::info("Computer({}): Reduced data: {}", m_ID, msg.m_data);
        }
        else if(Network::Messages::e_Type::ReduceAll == anyMsg->type()) {
            const auto &msg = *static_cast<const Network::Messages::ReduceAll *>(anyMsg.release());

            spdlog::info("Computer({}): Received reduce-all message!", m_ID);
            spdlog::info("Computer({}): Reduced data: {}", m_ID, msg.m_data);
        }
        else {
            spdlog::error("Computer({}): Cannot determine the type of received message!", m_ID);
            spdlog::debug("Type name was {}", anyMsg->typeToString());

            return false;
        }
    }

    return true;
}

bool Computer::isReady() const
{
    return m_port.isConnected();
}
