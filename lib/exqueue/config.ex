defmodule ExQueue.Config do
  import Logger, only: [log: 2]

  def read_config(nil, _f) do
    Application.get_env(:exqueue, :config, %{})
  end

  def read_config(spec, f) do
    case f.(spec) do
      {:ok, content} ->
        cfg = YamlElixir.read_from_string(content)
        read_config(nil, false) |>
          Map.merge(cfg)
      e ->
        log(:error, "Unable to derive config from #{inspect(spec)} : #{inspect(e)}")
        read_config(nil, false)
    end
  end

end
