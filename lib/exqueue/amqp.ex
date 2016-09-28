defmodule ExQueue.AmqpSup do
  use Supervisor
  def init(_args) do
    {:ok, []}
  end

  def start_link([cf, qa]) do
    c = [worker(ExQueue.Amqp, [], restart: :transient)]
    {:ok, sup_pid} = Supervisor.start_link(c, strategy: :simple_one_for_one)
    Agent.update(qa, fn m -> Map.put(m, :amqp, sup_pid) end)
    add_config(cf, qa)
    {:ok, sup_pid}
  end

  def cstart(sup_pid, args, sargs) do
    {:ok, cpid} = Supervisor.start_child(sup_pid, args)
  end

  def add_config(cf, qa) do
    sup_pid = Agent.get(qa, fn m -> Map.get(m, :amqp) end)
    Enum.map(cf, fn l -> cstart(sup_pid, [l, qa], [id: Map.get(l, "name")]) end)
  end
end

defmodule ExQueue.Amqp do
  import Logger, only: [log: 2]

  use GenServer

  def init([cf,sname,qa]) do
    {:ok, conn} = AMQP.Connection.open(cf["uri"])
    {:ok, chan} = AMQP.Channel.open(conn)
    AMQP.Basic.qos(chan, prefetch_count: 10)
    name = Map.get(cf, "name")
    exchange = Map.get(cf, "exchange", name) # use name if no explicit exch named
    node = Map.get(cf, "node", Agent.get(qa, fn m -> Map.get(m, :node) end))
    AMQP.Exchange.fanout(chan, exchange, durable: true)
    if Map.get(cf, "subscribe", true) do
      qname = exchange <> "_" <> node
      ttl = :timer.seconds(Map.get(cf, "expire", 3600))
      AMQP.Queue.declare(chan, qname, durable: true, arguments: ["x-message-ttl": ttl])
      AMQP.Queue.bind(chan, qname, exchange)
      {:ok, ctag} = AMQP.Basic.consume(chan, qname)
      log(:debug, "Consumer tag = #{inspect(ctag)}")
    end

    Agent.update(qa, fn m -> put_in(m,[:queues, name], %{ server: sname, conn: conn, chan: chan }) end)
    {:ok, %{ag: qa, conn: conn, chan: chan, exchange: exchange, name: name, me: node,
            buffer: ExQueue.MessageStash.get_value(name) }}
  end

  def start_link(cf = %{ "name" => n}, qa) when is_binary(n) do
    sname = (to_string(__MODULE__) <> "." <> n) |> String.to_atom
    GenServer.start_link(__MODULE__, [cf,sname,qa], name: sname)
  end

  def publish(s, obj) when is_binary(obj) do
    GenServer.call s, {:put, obj}
  end

  def killme(s) do
    GenServer.cast s, :killme
  end

  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, qa) do
    {:noreply, qa}
  end

  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, qa) do
    {:stop, :normal, qa}
  end

  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, qa) do
    {:noreply, qa}
  end

  def handle_info({:basic_deliver, payload, %{delivery_tag: tag, redelivered: _redelivered}}, st) do
    buffer = Map.get(st, :buffer, [])
    {:noreply, Map.put(st, :buffer, [ {tag, payload} | buffer ])}
  end

  def handle_cast(:killme, st) do
    raise "Killing #{st[:name]} by request"
    {:noreply, st}
  end

  def handle_call({:put, obj}, _from, st) when is_binary(obj) do
    r = AMQP.Basic.publish(Map.get(st, :chan), Map.get(st, :exchange), "", obj,
      persistent: true,
      message_id: :crypto.strong_rand_bytes(16))
    {:reply, r, st}
  end

  def handle_call({:get, num, _time}, _from, st) do
    buffer = Map.get(st, :buffer)
    r = Enum.take(Map.get(st, :buffer), num)
    buffer = Enum.drop(buffer, Enum.count(r))
    st = Map.put(st, :buffer, buffer)
    {:reply, r, st}
  end

  def handle_call({:ack, qidlist}, _from, st) do
    r = case Enum.map(qidlist, fn qid -> AMQP.Basic.ack(st[:chan], qid) end) |>
          Enum.all?(fn q -> q == :ok end) do
          true -> :ok
          _ -> :error
        end
    {:reply, r, st}
  end

  def handle_call({:nack, qidlist}, _from, st) do
    r = case Enum.map(qidlist, fn qid -> AMQP.Basic.reject(st[:chan], qid) end) |>
          Enum.all?(fn q -> q == :ok end) do
          true -> :ok
          _ -> :error
        end
    {:reply, r, st}
  end

  def terminate(_reason, st) do
    ExQueue.MessageStash.save_value(st[:name], st[:buffer])
  end
end
