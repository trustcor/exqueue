defmodule ExQueue.MessageStash do
  use GenServer
  import Logger, only: [log: 2]

  def init([]) do
    {:ok, %{}}
  end

  def start_link([]) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def save_value(name, buffer) do
    log(:debug, "Saving buffer #{inspect(buffer)} to stash for #{name}")
    GenServer.cast __MODULE__, {:put, name, buffer}
  end

  def get_value(name) do
    r = GenServer.call __MODULE__, {:get, name}
    log(:debug, "Retrieving #{inspect(r)} for #{name} from stash")
    r
  end

  def nonce_val(m) do
    case Poison.decode(m) do
      {:ok, v} -> get_in(v, ["attributes", "nonce"])
      {:error, _e} -> nil
    end
  end

  def handle_call({:get, name}, _from, bufs) do
    b = Map.get(bufs, name, [])
    Enum.each(b, fn {_qid, m} -> ConCache.delete(:exqueue_nonce_cache, nonce_val(m)) end)
    {:reply, b, Map.delete(bufs, name)}
  end

  def handle_cast({:put, name, buf}, bufs) do
    {:noreply, Map.put(bufs, name, buf)}
  end
end
