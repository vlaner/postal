# Postal is schema based message broker.
# Usage
## Connect with netcat
```bash
nc 127.0.0.1 8080
```

## Publishing messages
### Inside connection send following commands:
- Subscribe to topic.
    ```
    SUB <topic>
    ```
- Publish message to topic according to protocol.
    ```
    PUB <topic> <payload_length>
    <payload>
    ```
## Example publishing flow.
1. Subscribe to topic.
    ```
    SUB topic
    ```
2. Publish message.
    ```
    PUB topic 4
    data
    ```
- Example incoming message.
    ```
    MSG topic 805ab639ffc048c107ec21586d8bae90 4
    data
    ```
- Ack message with its ID.
    ```
    ACK 805ab639ffc048c107ec21586d8bae90
    ```

# Text based protocol
1. Subscribe
    ```
    SUB <topic>
    ```
- `<topic>`: Topic name.

2. Publish
    ```
    PUB <topic> <payload_length>
    <payload>
    ```
- `<topic>`: Topic name.
- `<payload_length>`: Length of payload bytes.
- `<payload>`: Actual payload in plain text.

3. Incoming Message
    ```
    MSG <topic> <message_id> <payload_length>
    payload
    ```
- `<topic>`: Topic name.
- `<message_id>`: ID of incoming message.
- `<payload_length>`: Length of payload bytes.
- `<payload>`: Actual payload in plain text.

4. Acknowledge
    ```
    ACK <message_id>
    ``` 
- `<message_id>`: ID of incoming message.