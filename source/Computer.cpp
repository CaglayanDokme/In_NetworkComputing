#include "Computer.hpp"
#include "spdlog/spdlog.h"
#include "Network/Message.hpp"
#include <fmt/core.h>

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
    m_statistics.timings.taskStart_tick = currentTick;

    std::vector<float> matrixA;
    std::vector<float> matrixB;

    if(0 == m_ID) {
        matrixA.resize(computingNodeAmount * computingNodeAmount);
        matrixB.resize(computingNodeAmount * computingNodeAmount);

        std::string matrixStrA;
        std::string matrixStrB;

        for(ssize_t row = 0; row < computingNodeAmount; ++row) {
            std::string rowStrA = "[ ";
            std::string rowStrB = "[ ";

            for(ssize_t col = 0; col < computingNodeAmount; ++col) {
                matrixA[(row * computingNodeAmount) + col] = row + col;
                matrixB[(row * computingNodeAmount) + col] = row - col;

                rowStrA += fmt::format("{:6.0f} ", matrixA[(row * computingNodeAmount) + col]);
                rowStrB += fmt::format("{:6.0f} ", matrixB[(row * computingNodeAmount) + col]);
            }

            rowStrA += "]";
            rowStrB += "]";

            matrixStrA += rowStrA + "\n";
            matrixStrB += rowStrB + "\n";
        }

        spdlog::debug("Matrix A:\n{}", matrixStrA);
        spdlog::debug("Matrix B:\n{}", matrixStrB);
    }

    m_mpi.scatter(matrixA, 0);
    m_mpi.broadcast(matrixB, 0);
    m_mpi.barrier();

    std::vector<float> localRow(computingNodeAmount);

    for(size_t colIdxB = 0; colIdxB < computingNodeAmount; ++colIdxB) {
        float sum = 0.0f;

        for(size_t idx = 0; idx < computingNodeAmount; ++idx) {
            const auto &colIdxA = idx;
            const auto &rowIdxB = idx;

            sum += matrixA[colIdxA] * matrixB[(rowIdxB * computingNodeAmount) + colIdxB];
        }

        const auto &localColIdx = colIdxB;
        localRow[colIdxB] = sum;
    }

    m_mpi.gather(localRow, 0);
    m_mpi.barrier();

    if(0 == m_ID) {
        std::string resultStr;
        for(size_t row = 0; row < computingNodeAmount; ++row) {
            std::string rowStr = "[ ";
            for(size_t col = 0; col < computingNodeAmount; ++col) {
                rowStr += fmt::format("{:6.0f} ", localRow[(row * computingNodeAmount) + col]);
            }
            rowStr += "]";

            resultStr += rowStr + "\n";
        }

        spdlog::debug("Result:\n{}", resultStr);
    }

    m_statistics.timings.taskEnd_tick = currentTick;
    spdlog::trace("Computer({}): Task finished..", m_ID);

    m_statistics.mpi = m_mpi.getStatistics(); // Synchronize statistics
    m_bDone = true;

    for( ; true; sleep(1));
}
