#pragma once

#include "Network/Message.hpp"
#include "ISwitch.hpp"
#include <map>

namespace Network::Switches {
    class Core : public ISwitch {
    public: /** Construction **/
        Core() = delete;
        explicit  Core(const std::size_t portAmount);

        // Forbid copying
        Core(const Core &) = delete;
        Core &operator=(const Core &) = delete;

        // Must be move-able to store in containers
        Core(Core &&) = default;
        Core &operator=(Core &&) = delete;

    public: /** Methods **/
        [[nodiscard]] bool tick() final;

    private: /** Members **/
        std::map<std::size_t, bool> m_barrierRequestFlags; // Key: Port index, Value: True/False

        struct {
            std::map<std::size_t, bool> flags;  // Key: Port index, Value: True/False
            std::size_t destinationID;          // ID of the destined computing node (i.e. root process of reduce operation)
            Reduce::OpType opType;              // Current operation type
            float value;                        // Current reduction value (e.g. Sum of received values, maximum of received values)
        } m_reduceStates;

        inline static std::size_t nextID = 0; // i.e. Number of core switches in total
    };
}
