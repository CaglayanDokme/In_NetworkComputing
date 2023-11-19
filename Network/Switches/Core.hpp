#pragma once

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

        inline static std::size_t nextID = 0; // i.e. Number of core switches in total
    };
}
