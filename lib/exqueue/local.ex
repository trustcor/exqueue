use Amnesia

defdatabase ExQueue.LocalDB do
  deftable Queues, [{:queue_id, autoincrement}, :name, :expire, :fexpire], type: :ordered_set, index: [:name] do
    @type t :: %Queues{queue_id: (integer | nil), name: String.t, expire: integer, fexpire: integer}
  end

  deftable Messages, [{:msg_id, autoincrement}, :umid, :queue_id, :status, :ts, :body, :attrs], type: :ordered_set, index: [:ts, :status, :queue_id, :umid] do
    @type t :: %Messages{msg_id: (integer | nil), umid: binary, queue_id: integer, status: atom,
			 ts: integer, body: String.t, attrs: keyword}
  end
end

defmodule ExQueue.LocalSup do
  use Supervisor
  
  def init(_args) do
    :ignore
  end

  def start_link([cf, qa]) do
    c = [worker(ExQueue.Local, [], restart: :transient)]
    {:ok, sup_pid} = Supervisor.start_link(c, strategy: :simple_one_for_one)
    Agent.update(qa, fn m -> Map.put(m, :local, sup_pid) end)
    add_config(cf, qa)
    {:ok, sup_pid}
  end

  def cstart(sup_pid, cf, qa) do
    {:ok, _cpid} = Supervisor.start_child(sup_pid, [cf, qa])
  end

  def add_config(cf, qa) do
    sup_pid = Agent.get(qa, fn m -> Map.get(m, :local) end)
    Enum.map(cf, fn l -> cstart(sup_pid, l, qa) end)
  end
end

defmodule ExQueue.Local do
  # import Logger, only: [log: 2]

  use GenServer
  use ExQueue.LocalDB

  def make_db() do
    ExQueue.LocalDB.create!(disk: [node])
    :ok = ExQueue.LocalDB.wait(15000)
  end
  
  def initialize_queue(q, opts \\ []) when is_list(opts) do
    expire = Keyword.get(opts, :expire, 3600) # default 1 hour expiry time
    fexpire = Keyword.get(opts, :flight_expire, 60) # default 60 seconds to acknowledge receipt of entry
    qid = Amnesia.transaction do
      qid = (Queues.where name == q, select: queue_id) |> Amnesia.Selection.values
      case qid do
	[] -> # create new queue
	  %Queues{name: q, expire: expire, fexpire: fexpire} |> Queues.write
	  (Queues.where name == q, select: queue_id) |>
	    Amnesia.Selection.values |> List.first
	[id] when is_integer(id) -> id
      end
    end
  end

  def init([cf,sname,qa]) do
    name = Map.get(cf, "name")
    {:ok, %{ qid: initialize_queue(name) }}
  end

  def start_link(cf = %{ "name" => n}, qa) do
    sname = (to_string(__MODULE__) <> "." <> n) |> String.to_atom
    GenServer.start_link(__MODULE__, [cf,sname,qa], name: sname)
  end

  @spec process_encoding(s :: any) :: (String.t | nil)
  defp process_encoding("raw"), do: "raw"
  defp process_encoding("base64"), do: "base64"
  defp process_encoding(_), do: nil
  
  @spec process_node(s :: any) :: (String.t | nil)
  defp process_node(n) when is_binary(n), do: n
  defp process_node(_), do: nil

  @spec process_nonce(s :: any) :: (String.t | nil)
  defp process_nonce(n) when is_binary(n), do: n
  defp process_nonce(_), do: nil

  @spec process_timestamp(s :: any) :: (integer | nil)
  defp process_timestamp(ts) when is_binary(ts) do
    case Timex.parse(ts, "{ISO:Extended:Z}") do
      {:ok, dt} -> DateTime.to_unix(dt)
      {:error, e} -> nil
    end
  end
  defp process_timestamp(_), do: nil

  @spec process_umid(s :: any) :: (binary | nil)
  defp process_umid(s) when is_binary(s) do
    # really dislike that UUID lib doesn't give an error
    # returning version of s2b. Relying on exception handling
    # sucks.
    try do
      UUID.string_to_binary!(s)
    rescue
      ArgumentError -> nil
    end
  end
  defp process_umid(_), do: nil
  
  @spec make_attrs(m :: map()) :: keyword()
  def make_attrs(m = %{}) do
    process_keys = %{
      "encoding" => {&process_encoding/1, :encoding},
      "nonce" => {&process_nonce/1, :nonce},
      "node" => {&process_node/1, :node},
      "timestamp" => {&process_timestamp/1, :ts},
      "messageid" => {&process_umid/1, :umid}
    }
    m |>
      Enum.filter(
	fn {k, _} when is_binary(k) -> Map.has_key?(process_keys, String.downcase(k))
	  _ -> false end) |>
      Enum.map(fn {k, v} -> {cv, kw} = Map.get(process_keys, String.downcase(k)); {kw, cv.(v)} end) |>
      Enum.reject(fn {_k, v} -> v == nil end) |>
      Enum.sort
  end
  
  @spec add_message(q :: String.t, body :: String.t, attrs :: keyword) :: ( %Messages{} | {:error, atom } )
  def add_message(q, body, attrs \\ []) do
    ts = Keyword.get(attrs, :ts, 0)
    ts = case ts do
	   0 -> (DateTime.utc_now |> DateTime.to_unix) # default == now
	   t -> t
	 end
    um = Keyword.get(attrs, :umid, "")
    attrs = Keyword.delete(attrs, :ts) |> Keyword.delete(:umid)
    Amnesia.transaction do
      qid = (Queues.where name == q, select: queue_id) |> Amnesia.Selection.values
      case qid do
	[] -> {:error, :no_such_queue}
	[id] when is_integer(id) ->
	  case Messages.where(umid == um, select: msg_id, limit: 1) |> Amnesia.Selection.values do
	    [id] when is_integer(id) ->
	      {:error, :message_not_unique}
	    [] -> %Messages{queue_id: id, umid: um, body: body, ts: ts, attrs: attrs, status: :ready} |>
		Messages.write
	  end
      end
    end
  end

  @spec queue_empty(q :: String.t) :: boolean
  def queue_empty(q) do
    Amnesia.transaction do
      qid = (Queues.where name == q, select: queue_id) |> Amnesia.Selection.values
      case qid do
	[] -> {:error, :no_such_queue}
	[id] when is_integer(id) ->
	  qe = (Messages.where(queue_id == id, select: msg_id, limit: 1)) |> Amnesia.Selection.values
	  qe == []
      end
    end
  end

  @spec get_messages(q :: String.t, limit :: integer) :: [ %Messages{} ] | {:error, atom}
  def get_messages(q, limit) do
    Amnesia.transaction do
      qid = (Queues.where name == q, select: queue_id) |> Amnesia.Selection.values
      case qid do
	[] -> {:error, :no_such_queue}
	[id] when is_integer(id) ->
	  ms = (Messages.where(queue_id == id and status == :ready, limit: limit)) |>
	    Amnesia.Selection.values
	  Enum.each(ms, fn m -> %Messages{m | status: :in_flight} |> Messages.write end)
	  ms
      end
    end
  end
  
end
