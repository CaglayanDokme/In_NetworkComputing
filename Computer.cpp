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

        if(anyMsg->type() == typeid(Network::Message)) {
            const auto msg = std::any_cast<Network::Message>(anyMsg.release());
            spdlog::trace("Computer({}): Received message from node #{}", m_ID, msg->m_sourceID);

            if(msg->m_destinationID != m_ID) {
                spdlog::error("Computer({}): Message delivered to wrong computer! Actual destination was #{}", m_ID, msg->m_destinationID);
            }
        }
        else if(anyMsg->type() == typeid(Network::BroadcastMessage)) {
            auto msg = std::any_cast<Network::BroadcastMessage>(anyMsg.release());

            spdlog::trace("Computer({}): Received broadcast message from node #{}", m_ID, msg->m_sourceID);
        }
        else if(anyMsg->type() == typeid(Network::BarrierRelease)) {
            spdlog::debug("Computer({}): Barrier release received!", m_ID);
        }
        else {
            spdlog::error("Computer({}): Cannot determine the type of received message!", m_ID);
            spdlog::debug("Type name was {}", anyMsg->type().name());

            return false;
        }
    }

    return true;
}

bool Computer::isReady() const
{
    return m_port.isConnected();
}
