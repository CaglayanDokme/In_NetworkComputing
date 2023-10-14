#pragma  once

#include <deque>
#include <cstddef>

namespace Network {
    struct Message {
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

    class Port {
    public: /** Aliases **/
        using MsgType = Message;

    public: /** Construction **/
        Port() = default;

        // Cannot be copied
        Port(const Port &) = delete;
        Port &operator=(const Port &) = delete;

        // Must be move-able to store in containers
        Port(Port &&) noexcept = default;
        Port &operator=(Port &&) = default;

    public: /** Methods **/
        void tick();

        /**
         * @brief  Set connection between this port and given remote port
         * @param  remotePort Port to be connected with
         * @return True       If connected successfully
         *
         * @note This method modifies the given remote port.
         */
        bool connect(Port &remotePort);

        /**
         * @brief Check if the port has a proper connection
         * @return True If the port is connected to a remote port and the remote port verifies it
         */
        [[nodiscard]] bool isConnected() const;

        /**
         * @brief Push a message to port's outgoing queue
         * @param msg Read-only reference to copiable message
         */
        void pushOutgoing(const MsgType &msg);

        /**
         * @brief  Get the amount of outgoing messages
         * @return Current amount of messages in the outgoing queue
         */
        [[nodiscard]] std::size_t outgoingAmount() const;

        /**
         * @brief Pop an incoming message from the port
         * @return A message instance
         *
         * @note
         */
        [[nodiscard]] MsgType popIncoming();

        /**
         * @brief Check if the port has a ready-to-fetch incoming message
         * @return True If at least one message can be fetched
         */
        [[nodiscard]] bool hasIncoming() const;

    private:
        /**
         * @brief Push an incoming message to port
         * @param msg Read-only reference to copiable message
         *
         * @note This method can only be called from another port which has an outgoing message
         */
        void pushIncoming(const MsgType &msg);

    private: /** Members **/
        Port *m_pRemotePort{nullptr};

        struct st_Msg {
            st_Msg(const MsgType &data, const std::size_t &delay);

            MsgType data;
            std::size_t remaining;  // Remaining ticks to fetch
        };

        std::deque<st_Msg> m_incoming; // Messages from other ports
        std::deque<st_Msg> m_outgoing; // Messages to other ports
    };
}