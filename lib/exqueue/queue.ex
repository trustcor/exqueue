defmodule ExQueue.Queue do
  @moduledoc """
  Top level abstraction for API
  """

  import Logger, only: [log: 2]

  use GenServer

  def init([qa]) do
    {:ok, qa}
  end

  def start_link(qa) do
    GenServer.start_link(__MODULE__, [qa], name: __MODULE__)
  end

  defp nodename(qa), do: Agent.get(qa, fn m -> Map.get(m, :node, "nodename") end)

  def publish(queues, msg) when is_list(queues) and is_binary(msg) do
    publish(queues, msg, %{})
  end

  def publish(queues, msg, attrs = %{}) when is_list(queues) and is_binary(msg) do
    GenServer.call __MODULE__, {:publish, queues, msg, attrs}
  end

  def receive_messages(queues, nmesg \\ 1, time \\ 5)  when is_list(queues) do
    GenServer.call __MODULE__, {:receive, queues, nmesg, time}
  end

  defp encode_message(true, msg), do: {"raw", msg}
  defp encode_message(false, msg), do: {"base64", Base.encode64(msg)}

  defp decorate_message(msg, nodename, attrs = %{}) when is_binary(msg) do
    {enc, message} = encode_message(String.printable?(msg), msg)
    attrs = Map.put(attrs, "encoding", enc) |>
      Map.put("node", nodename) |>
      Map.put("timestamp", DateTime.utc_now |> DateTime.to_iso8601) |>
      Map.put("nonce", (:crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)))
    Poison.encode!(%{body: message, attributes: attrs})
  end

  defp undecorate_message(qname, qid, msg) do
    {body, attributes} = case Poison.decode(msg) do
                           {:ok,
                            %{
                              "body" => b,
                              "attributes" => a = %{
                                "nonce" => n,
                                "timestamp" => ts,
                                "node" => nname,
                                "encoding" => e
                              }
                            }
                           } when is_binary(b) and is_binary(n) and is_binary(ts) and is_binary(n) and is_binary(e) and is_binary(nname) ->
                             case e do
                               "base64" -> case Base.decode64(b) do
                                             :error ->
                                               log(:error, "Invalid base64 encoding in message body")
                                               {nil, %{}}
                                             {:ok, m} ->
                                               {m, a}
                                           end
                               _ -> {b, a}
                             end
                           {:ok, m} ->
                             log(:error, "JSON body is not in correct format { body + attrs }: #{inspect(msg)} -> #{inspect(m, pretty: true)}")
                             {nil, %{}}
                           {:error, e} ->
                             log(:error, "Body is not well formed JSON structure: #{inspect(e)}")
                             {nil, %{}}
                           :error ->
                             log(:error, "Body is not well formed JSON structure")
                             {nil, %{}}
                         end

    { qname, qid, body, attributes }
  end

  defp queue_servers(qa, queues) do
    Enum.map(queues,
      fn q -> case Agent.get(qa, fn m -> get_in(m, [:queues, q, :server]) end) do
                nil ->
                  log(:error, "Unable to find server for queue named #{inspect(q)}")
                  {q, nil}
                s ->
                  {q, s}
              end
      end)
  end

  def queues() do
    GenServer.call __MODULE__, :queues
  end

  defp tag_response(a, skew), do: tag_response(a, skew, DateTime.utc_now)

  defp tag_response(%{ "timestamp" => ts, "nonce" => n }, skew, now) when is_binary(ts) and is_binary(n) do
    case Timex.parse(ts, "{ISO:Extended:Z}") do
      {:error, e} ->
        log(:warn, "Parsing timestamp #{ts} yields error: #{inspect(e)}")
        :invalid
      {:ok, dt} ->
        diff = abs(Timex.diff(now, dt, :seconds))
        if diff > skew do
          log(:warn, "Time difference = #{diff}, max skew allowed = #{skew}")
          :invalid # date is too old or too new
        else
          case ConCache.insert_new(:exqueue_nonce_cache, n, %ConCache.Item{value: true, ttl: :timer.seconds(skew * 2)}) do
            {:error, _} -> :duplicate
            _ -> :ok
          end
        end
    end
  end

  defp tag_response(_, _, _), do: :invalid

  defp filter_response(:invalid, {q, qid, _msg, _attrs}), do: {q, qid}
  defp filter_response(:duplicate, {q, qid, _msg, _attrs}), do: {q, qid}
  defp filter_response(_, m), do: m

  defp partition_replies(rl, skew) when is_list(rl) do
    Enum.map(rl, fn {q, qid, msg, attrs} -> {tag_response(attrs, skew), q, qid, msg, attrs} end) |>
      Enum.group_by(fn {res, _q, _qid, _msg, _attrs} -> res end, fn {_res, q, qid, msg, attrs} -> {q, qid, msg, attrs} end) |>
      Enum.map(fn {k, rl} when is_list(rl) -> {k, Enum.map(rl, fn r -> filter_response(k, r) end)} end) |>
      Enum.map(fn {k, rl} -> {k, Enum.group_by(rl, fn t -> elem(t, 0) end,
                              fn {_q, qid, msg, attrs} -> {qid, msg, attrs}
                                             {_q, qid} -> qid end)} end)
  end

  def qids(messages) do
    GenServer.call __MODULE__, {:qids, messages}
  end

  def ack(qids, timeout \\ 5) do
    GenServer.call __MODULE__, {:ack, qids, timeout}
  end

  def nack(qids, timeout \\ 5) do
    GenServer.call __MODULE__, {:nack, qids, timeout}
  end

  # handlers below

  def handle_call({:receive, queues, nmesg, time}, _from, qa) do
    qs = queue_servers(qa, queues) |>
      Enum.reject(fn {_q, s} -> s == nil end) |>
      Enum.map(fn {q,s} -> {q, Task.async(fn -> GenServer.call(s, {:get, nmesg, time}) end)} end) |>
      Enum.map(fn {q,p} -> {q, Task.await(p, :timer.seconds(time))} end) |>
      Enum.map(fn {q, ml} -> Enum.map(ml, fn {qid,msg} -> undecorate_message(q, qid, msg) end) end) |>
      List.flatten |>
      partition_replies(Agent.get(qa, fn m -> Map.get(m, :max_age, 86400) end))
    {:reply, qs, qa}
  end

  def handle_call({:publish, queues, msg, attrs}, _from, qa) do
    body = decorate_message(msg, nodename(qa), attrs)
    qs = queue_servers(qa, queues) |>
      Enum.reject(fn {_q,s} -> s == nil end) |>
      Enum.map(fn {_q,s} -> GenServer.call(s, {:put, body}) end)

    {:reply, qs, qa}
  end

  def handle_call(:queues, _from, qa) do
    {:reply, Agent.get(qa, fn m -> Map.keys(Map.get(m, :queues, %{})) end), qa}
  end

  def handle_call({:qids, replies}, _from, qa) do
    qidlist = Enum.map(replies, fn {_st, rm} ->
      Enum.map(rm,
        fn { broker, vals } ->
          Enum.map(vals, fn {qid, _msg, _attrs} -> {broker, qid}
            qid -> {broker, qid} end) end) end) |>
      List.flatten |>
      Enum.group_by(fn {q, _m} -> q end, fn {_q, m} -> m end)
    {:reply, qidlist, qa}
  end

  def handle_call({:ack, qidlist, timeout}, _from, qa) do
    qs = queue_servers(qa, Map.keys(qidlist)) |>
      Enum.reject(fn {_q, s} -> s == nil end) |>
      Enum.map(fn {q, s} -> {q, Task.async(fn -> GenServer.call(s, {:ack, Map.get(qidlist, q, [])}) end)} end) |>
      Enum.map(fn {q, p} -> {q, Task.await(p, :timer.seconds(timeout))} end) |>
      List.flatten |>
      Enum.into(%{})
    {:reply, qs, qa}
  end

  def handle_call({:nack, qidlist, timeout}, _from, qa) do
    qs = queue_servers(qa, Map.keys(qidlist)) |>
      Enum.reject(fn {_q, s} -> s == nil end) |>
      Enum.map(fn {q, s} -> {q, Task.async(fn -> GenServer.call(s, {:nack, Map.get(qidlist, q, [])}) end)} end) |>
      Enum.map(fn {q, p} -> {q, Task.await(p, :timer.seconds(timeout))} end) |>
      List.flatten |>
      Enum.into(%{})
    {:reply, qs, qa}
  end
end
