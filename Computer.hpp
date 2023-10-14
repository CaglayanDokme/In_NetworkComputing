#pragma once

#include "Network/Port.hpp"

class Computer {
public: /** Construction **/
    Computer();

    // Forbid copying
    Computer(const Computer &) = delete;
    Computer &operator=(const Computer &) = delete;

    // Must be move-able to store in containers
    Computer(Computer &&) noexcept = default;
    Computer &operator=(Computer &&) = delete;

public: /** Methods **/
    [[nodiscard]] bool tick();

    /**
     * @brief  Get the unique ID of the computing node.
     * @return Unique ID computing nodes
     */
    [[nodiscard]] std::size_t getID() const { return m_ID; }

    /**
     * @brief  Get the port of the computing node
     * @return Reference to the contained port
     */
    [[nodiscard]] Network::Port &getPort() { return m_port; }

    /**
     * @brief  Check if the computing node has been initialized properly
     * @return True If the connection port is connected to another port
     */
    [[nodiscard]] bool isReady() const;

private: /** Members **/
    const std::size_t m_ID;
    Network::Port m_port;

    // Static
    inline static std::size_t nextID = 0; // i.e. Number of computing nodes in total
};
