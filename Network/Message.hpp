#pragma once

#include <cstddef>

namespace Network {
    class Message {
    public: /** Construction **/
        Message() = delete;
        Message(const std::size_t sourceID, const std::size_t destinationID);

    public: /** Addressing **/
        const std::size_t m_sourceID;
        const std::size_t m_destinationID;

    public: /** Data **/
        struct {
            float data;
        } m_data;
    };
};
