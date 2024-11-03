#include "Computer.hpp"
#include "spdlog/spdlog.h"
#include "Network/Message.hpp"

Computer::Computer()
: m_ID(nextID++), m_mpi(m_ID)
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

void Computer::setTotalAmount(const size_t totalAmount)
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
    std::call_once(m_tickStarted, [this] {
        m_task = std::thread(&Computer::task, this);
        m_task.detach();
    });

    // Advance port
    m_mpi.tick();

    // Synchronize statistics
    m_statistics.mpi = m_mpi.getStatistics();

    if(m_ID == (computingNodeAmount - 1)) {
        ++currentTick;
    }

    return true;
}

bool Computer::isReady() const
{
    return m_mpi.isReady();
}

void Computer::task()
{
    spdlog::trace("Computer({}): Task started..", m_ID);

    m_mpi.barrier();

    spdlog::trace("Computer({}): Task finished..", m_ID);
    m_bDone = true;

    for( ; true; sleep(1));
}
