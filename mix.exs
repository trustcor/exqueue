defmodule ExQueue.Mixfile do
  use Mix.Project

  def project do
    [app: :exqueue,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     escript: [main_module: ExQueue],
     aliases: aliases,
     deps: deps()]
  end

  def application do
    [applications: [:logger, :yamerl, :yaml_elixir, :con_cache, :poison,
                    :timex, :amqp, :ex_aws, :sweet_xml, :uuid],
     included_applications: [:mnesia, :amnesia],
    mod: {ExQueue, %{}}
    ]
  end

  defp aliases do
    [
      test: "test --no-start"
    ]
  end
  
  defp deps do
    [
      {:yaml_elixir, "~> 1.2.1"},
      {:con_cache, "~> 0.11.1"},
      {:poison, "~> 2.2.0"},
      {:timex, "~> 3.1.0"},
      {:amqp_client, git: "https://github.com/dsrosario/amqp_client.git", branch: "erlang_otp_19", override: true},
      {:amqp, "~> 0.1.4"},
      {:ex_aws, "~> 1.0.0-rc.4"},
      {:sweet_xml, "~> 0.6.1"},
      {:briefly, "~> 0.3.0", only: [:test]},
      {:dialyxir, "~> 0.4", only: [:dev], runtime: false},
      {:amnesia, "~> 0.2.5"},
      {:uuid, "~> 1.1" }
    ]
  end
end
