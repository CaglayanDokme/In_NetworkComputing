#pragma once

#include "Network/Port.hpp"
#include "Network/MPI.hpp"
#include <thread>

class Computer {
public: /** Struct Declarations **/
    struct Statistics  {
        Network::MPI::Statistics mpi;

        struct Timings {
            size_t taskStart_tick;
            size_t taskEnd_tick;
            size_t taskDuration() const { return taskEnd_tick - taskStart_tick; }
        } timings;
    };

public: /** Construction **/
    Computer();

    // Forbid copying
    Computer(const Computer &) = delete;
    Computer &operator=(const Computer &) = delete;

    // Must be move-able to store in containers
    Computer(Computer &&) noexcept = default;
    Computer &operator=(Computer &&) = delete;

public: /** Methods **/
    /**
     * @brief Initially and once, set the total amount of computing nodes to be spawned
     * @param totalAmount Total amount of computing nodes
     *
     * @attention This function must be called once prior to spawning any computing node.
     */
    static void setTotalAmount(const size_t totalAmount);

    [[nodiscard]] bool tick();

    /**
     * @brief  Get the unique ID of the computing node.
     * @return Unique ID computing nodes
     */
    [[nodiscard]] size_t getID() const { return m_ID; }

    /**
     * @brief  Get the port of the computing node
     * @return Reference to the contained port
     */
    [[nodiscard]] Network::Port &getPort() { return m_mpi.getPort(); }

    /**
     * @brief  Check if the computing node has been initialized properly
     * @return True If the connection port is connected to another port
     */
    [[nodiscard]] bool isReady() const;

    /**
     * @brief  Check if the computing node has finished its task
     * @return True If the computing node has finished its task
     */
    [[nodiscard]] bool isDone() const { return m_bDone; }

    /**
     * @brief  Get the statistics of the computing node
     * @return Object containing the statistics
     */
    [[nodiscard]] const Statistics &getStatistics() const { return m_statistics; }

private:
    /**
     * @brief Main computing logic of the computing node
     */
    void task();

private: /** Members **/
    const size_t m_ID;

    Network::MPI m_mpi;
    std::thread m_task;
    std::once_flag m_tickStarted;
    Statistics m_statistics;
    bool m_bDone{false};

    // Static
    inline static size_t computingNodeAmount = 0;  // Number of computing nodes to be spawned (Should be set initially)
    inline static size_t nextID = 0;               // i.e. Number of spawned(up to now) computing nodes in total
    inline static size_t currentTick = 0;          // Current time of the simulation
};
