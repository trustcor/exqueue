# Exqueue

A simple queue abstraction layer with drivers for:

 - Amazon AWS SQS
 - AMQP (e.g. RabbitMQ)
 - Local file based queuing

Queue using apps can publish and consume from multiple queues (mixing
drivers if desired), and be assured that only unique messages will be
returned to the application.

ExQueue is designed to tolerate broker failure, and will restart
consumers if remote failure is determined.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `exqueue` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:exqueue, "~> 0.1.0"}]
    end
    ```

  2. Ensure `exqueue` is started before your application:

    ```elixir
    def application do
      [applications: [:exqueue]]
    end
    ```

## Configuration

Configuration is done via YAML or via Elixir Application config.

An example configuration looks like this:

   ```yaml
   ---
   node: mynodename # the name of this node (used to differentiate remote queues)
   local: # driver for local driver
     - name: localq # the name of the queue to the application
       path: /tmp/queues/tq1 # path to a directory holding the queue data
       options:
         - expire: 3600 # how long until messages auto-clear (default 7200s)
         - flight_expire: 10
           # how long before un-acked messages become visible again (def 30s)
         - flight_cycle: 20
           # how long between cycles which transfer un-acked messages
           # back into the main queue for reprocessing
   amqp:
     - name: amqp1
       uri: amqp://<AMQP user>:<password>@amqp-broker.my.domain
       exchange: exqueue_test1 # explicitly set exchange name
       # if not explicitly set, it would be the same as
       # the `name` parameter
       expire: 30 # Message TTL - default 7200s

   aws:
     - name: awstest-use1 # name of queue for client to use
       region: us-east-1 # AWS region (default: us-east-1)
       topic: awstest # SNS topic for publication (default == name value)
       access_key_id: <AWS access id>
       secret_access_key: <AWS secret key>
       expire: 300 # Message TTL - default 300s
   ```
   
## Example Usage


   ```iex
   iex(1)> ExQueue.main(["-c", "config/config.yml"])
   {:ok, #PID<0.250.0>}
   
   iex(2)> ExQueue.Queue.queues
   ["amqp1", "awstest-use1", "localq"]
   
   iex(3)> ExQueue.Queue.publish(["amqp1", "awstest-use1"], "Queue message 1")
   [:ok, :ok]
   
   iex(4)> r = ExQueue.Queue.receive_messages(["amqp1", "awstest-use1"], 5)
    # 5 messages per driver
    [duplicate: %{"awstest-use1" => ["AQEBywW1ZhZ6mVJDwKKV+vdrvEQ+RURlhXDln8kzAIvajtWS7iQ9TUJMMfECsCxFuwiCQTXCvGhAoC6E/a9Qc3rt3JgDstrsnTPhu4LUmqgZAaZPT0d/4hAdgOHCaKbzPZ75pzXu8alz0wwrGE5o54jfxVSOvCuHbCC8LD5r+Rjd2//uBfYWW8ApmfRpmujR0wAySiUAHgXu1FRbLZZr+HAdHF3gPrnkFzwQjHNizc4JFUqEHRAj0SGy0M9uk5I5Bs3e0UZb5kUmNuLEyBObC3Zz6i2SbgSxlfZ30aTVwtXRe6ZyNAJ9zAezMX7L4j9gTWLkZK7irAO7V2jErdNL8/J8szPR5vKBx/RgSV3FtMGQXfVnvoXh2HjoNHQD+X9brRYE,16565933-cdbb-4a76-8aa5-4c810c64b119"]},
    ok: %{"amqp1" => [{1, "Queue message 1",
     %{"encoding" => "raw", "node" => "mynodename",
       "nonce" => "55d4c58d09c4fc72dc58a77032da4d6d",
       "timestamp" => "2016-09-26T12:52:59.638047Z"}}]}]
       
   iex(5)> {_id, body, attrs} = get_in(r, [:ok, "amqp1"]) |> List.first
   {1, "Queue message 1",
   %{"encoding" => "raw",
   "node" => "mynode",
   "nonce" => "55d4c58d09c4fc72dc58a77032da4d6d",
   "timestamp" => "2016-09-26T12:52:59.638047Z"}}
   
   iex(6)> body
   "Queue message 1"
   
   iex(7)> ql = ExQueue.Queue.qids(r)
    %{"amqp1" => [1],
    "awstest-use1" =>
    ["AQEBywW1ZhZ6mVJDwKKV+vdrvEQ+RURlhXDln8kzAIvajtWS7iQ9TUJMMfECsCxFuwiCQTXCvGhAoC6E/a9Qc3rt3JgDstrsnTPhu4LUmqgZAaZPT0d/4hAdgOHCaKbzPZ75pzXu8alz0wwrGE5o54jfxVSOvCuHbCC8LD5r+Rjd2//uBfYWW8ApmfRpmujR0wAySiUAHgXu1FRbLZZr+HAdHF3gPrnkFzwQjHNizc4JFUqEHRAj0SGy0M9uk5I5Bs3e0UZb5kUmNuLEyBObC3Zz6i2SbgSxlfZ30aTVwtXRe6ZyNAJ9zAezMX7L4j9gTWLkZK7irAO7V2jErdNL8/J8szPR5vKBx/RgSV3FtMGQXfVnvoXh2HjoNHQD+X9brRYE,16565933-cdbb-4a76-8aa5-4c810c64b119"]}
   
   iex(8)> ExQueue.Queue.ack(ql)
   %{"amqp1" => :ok, "awstest-use1" => :ok}
   
   iex(9)> r = ExQueue.Queue.receive_messages(["amqp1", "awstest-use1"])
   []
   ```
   
 If a consumer does not wish to acknowledge (and thus delete) message
 from a queue, it can use the `ExQueue.Queue.nack(<Queue ID list>)` call
 using the same parameters as `ExQueue.Queue.ack(<Queue ID list>)`; this
 returns the message(s) to the queue to be processed by other list
 consumers.
 
## Message Status

When messages are received from a broker, each message is categorized as
follows:

  - `ok`: the message is new and has been delivered in time. The body and
        message attributes of the message are preserved, as well as the
        queue ID for subsequent ack'ing or nack'ing
  - `duplicate`: this message has been received before (or is delivered as
        part of the existing message set)
  - `invalid`: the format of the message was wrong (perhaps not published
        via `ExQueue.Queue.Publish`) or its timestamp marks it as too
        old for processing. The maximum age of a valid message is set in
        the config as the `max_age` parameter, and by default is 86400
        seconds.
        
Note that if two queues deliver the same message (identified by having
the same 128-bit random nonce), it is not defined as to which queue will
yield the `ok` result and which the `duplicate` one.

Received messages can be turned into a list of message IDs suitable for
ack'ing or nack'ing via the `ExQueue.Queue.qids` utility function.

## Message Contents

Messages which can be displayed as UTF-8 are stored as-is (ie, raw
encoding). Messages which are not displayable are converted to base 64,
and their encoding set to 'base64' in the message attributes.
