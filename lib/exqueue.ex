defmodule ExQueue do
  use Application

  import Logger, only: [log: 2]

  def main(argv) do
    {options, _, _} = OptionParser.parse(argv,
      switches: [config: :string],
      aliases: [c: :config]
    )

    st = ExQueue.start(:normal)
    if options[:config] do
      ExQueue.add_yaml_file_config(options[:config])
    end
    st
  end

  defp get_in_default(m, keys, default) do
    case get_in(m, keys) do
      nil -> default
      x -> x
    end
  end

  def start(type), do: start(type, %{})

  def start(_type, cfg = %{}) do
    import Supervisor.Spec, warn: false

    {:ok, qa} = Agent.start_link(fn -> %{ queues: %{}, node: Map.get(cfg, "node", "nodename"),
                                          max_age: Map.get(cfg, "max_age", 86400)} end,
      name: __MODULE__.Agent)

    ttl = get_in_default(cfg, ["config", "nonce_ttl"], Application.get_env(:exqueue, :nonce_ttl, 7200))

    children = [
      worker(ConCache, [[ttl_check: :timer.seconds(5), ttl: :timer.seconds(ttl)], [name: :exqueue_nonce_cache]]),
      worker(ExQueue.MessageStash, [[]]),
      worker(ExQueue.LocalSup, [[Map.get(cfg, "local", %{}), qa]]),
      worker(ExQueue.AmqpSup, [[Map.get(cfg, "amqp", %{}), qa]]),
      worker(ExQueue.AwsSup, [[Map.get(cfg, "aws", %{}), qa]]),
      worker(ExQueue.Queue, [qa]),
    ]

    opts = [strategy: :one_for_one, name: ExQueue.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def add_config(cfg = %{}) do
    qa = Process.whereis(__MODULE__.Agent)
    Agent.update(qa, fn m -> Map.put(m, :node, Map.get(cfg, "node", "nodename")) end)
    ExQueue.LocalSup.add_config(Map.get(cfg, "local", %{}), qa)
    ExQueue.AmqpSup.add_config(Map.get(cfg, "amqp", %{}), qa)
    ExQueue.AwsSup.add_config(Map.get(cfg, "aws", %{}), qa)
  end

  def add_yaml_config(ycf) do
    ExQueue.Config.read_config(ycf) |> add_config
  end

  def add_yaml_file_config(yf) do
    ExQueue.Config.read_config(yf, &File.read/1) |> add_config
  end
end

defmodule ExQueue.StressTest do
  import Logger, only: [log: 2]
  alias Experimental.Flow

  @test_queues ["awstest-eu1", "awstest-eu2"]
  @test_config "config/stress.yml"

  defp pubm(queues, i) do
    ExQueue.Queue.publish(queues, "Publishing message #{i}")
    i
  end

  defp drain_queues(queues) do
    case ExQueue.Queue.receive_messages(queues, 10) do
      [] ->
        nil
      l ->
        ok = Keyword.get(l, :ok, []) |>
          Enum.map(fn {_q, qm} -> Enum.map(qm, fn {_id, m, _attrs} -> m end) end) |>
          List.flatten
        case ok do
          [] -> nil
          ok ->
            log(:debug, "Got #{Enum.count(ok)} messages from #{inspect(queues)}")
        end
        ExQueue.Queue.qids(l) |> ExQueue.Queue.ack
    end
    drain_queues(queues)
  end

  def test_pub_sub(opts \\ []) do
    num = Keyword.get(opts, :num, 10)
    queues = Keyword.get(opts, :queues, @test_queues)
    Flow.from_enumerable(1..num) |> Flow.map(fn i -> pubm(queues, i) end) |> Enum.sort()
    log(:info, "Test publish complete.")
    :ok
  end

  def tmain(opts \\ []) do
    ExQueue.main(["-c", @test_config])
    queues = Keyword.get(opts, :queues, @test_queues)
    spawn fn -> drain_queues(queues) end
  end
end
