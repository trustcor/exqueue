defmodule ExQueue do
  use Application

  # import Logger, only: [log: 2]

  def main(argv) do
    {options, _, _} = OptionParser.parse(argv,
      switches: [config: :string],
      aliases: [c: :config]
    )

    cfg = ExQueue.Config.read_config(options[:config], &File.read/1)
    ExQueue.start(:normal, cfg)
  end

  defp get_in_default(m, keys, default) do
    case get_in(m, keys) do
      nil -> default
      x -> x
    end
  end

  def start(_type, cfg = %{}) do
    import Supervisor.Spec, warn: false

    {:ok, qa} = Agent.start_link(fn -> %{ queues: %{}, node: Map.get(cfg, "node", "nodename"),
                                          max_age: Map.get(cfg, "max_age", 86400)} end)

    ttl = get_in_default(cfg, ["config", "nonce_ttl"], Application.get_env(:exqueue, :nonce_ttl, 7200))

    children = [
      worker(ConCache, [[ttl_check: :timer.seconds(5), ttl: :timer.seconds(ttl)], [name: :nonce_cache]]),
      worker(ExQueue.MessageStash, [[]]),
      worker(ExQueue.LocalSup, [[Map.get(cfg, "local", %{}), qa]]),
      worker(ExQueue.AmqpSup, [[Map.get(cfg, "amqp", %{}), qa]]),
      worker(ExQueue.AwsSup, [[Map.get(cfg, "aws", %{}), qa]]),
      worker(ExQueue.Queue, [qa]),
    ]

    opts = [strategy: :one_for_one, name: ExQueue.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
