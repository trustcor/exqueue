defmodule ExQueue.AwsSup do
  use Supervisor

  def init(_args) do
    {:ok, []}
  end

  def start_link([cf, qa]) do
    c = [worker(ExQueue.Aws, [], restart: :transient)]
    {:ok, sup_pid} = Supervisor.start_link(c, strategy: :simple_one_for_one)
    Agent.update(qa, fn m -> Map.put(m, :aws_sup_pid, sup_pid) end)
    add_config(cf, qa)
    {:ok, sup_pid}
  end

  def cstart(sup_pid, cf, qa) do
    {:ok, _cpid} = Supervisor.start_child(sup_pid, [cf, qa])
  end

  def add_config(cf, qa) do
    Enum.map(cf, fn l -> cstart(Agent.get(qa, fn m -> Map.get(m, :aws_sup_pid) end), l, qa) end)
  end
end

defmodule ExQueue.Aws do
  import Logger, only: [log: 2]
  import SweetXml

  use GenServer

  def init([cf, sname, qa]) do
    name = Map.get(cf, "name")
    topic = Map.get(cf, "topic", name)
    region = Map.get(cf, "region", "us-east-1")
    access = get_in(cf, ["access_key_id"])
    secret = get_in(cf, ["secret_access_key"])
    wait = Map.get(cf, "wait", 20)
    buffer_timeout = Map.get(cf, "buffer_timeout", 1800) # messages not claimed in 1800 secs get zapped
    buffer_purge_period = Map.get(cf, "buffer_purge_period", 60) # run purge every 60 seconds
    stats_log_period = Map.get(cf, "stats_log_period", 60)
    stats_log_level = Map.get(cf, "stats_log_level", "debug") |> log_level
    max_mess = Map.get(cf, "max_messages", 10)
    mangle_attrs = Map.get(cf, "mangle_attrs", true)

    node = Map.get(cf, "node", Agent.get(qa, fn m -> Map.get(m, :node) end))
    {:ok, tarn} = create_topic(topic, region, access, secret)
    qurl = if Map.get(cf, "subscribe", true) do
      qname = topic <> "_" <> node
      {:ok, q} = create_and_subscribe_queue({:ok, tarn}, qname, region, access, secret, ttl: Map.get(cf, "expire", 3600))
      me = self
      spawn_link(fn -> queue_poller(me, q, region, access, secret, max_mess, wait, mangle_attrs) end)
      q
    else
      nil
    end
    Agent.update(qa, fn m -> put_in(m,[:queues, name], %{ server: sname }) end)
    me = self()
    spawn_link(fn -> purger(me, buffer_purge_period) end)
    spawn_link(fn -> stats_log(me, name, stats_log_period, stats_log_level) end)
    {:ok, %{region: region, access: access, secret: secret, tarn: tarn, name: name,
            qurl: qurl, buffer_timeout: buffer_timeout,
            buffer:  ExQueue.MessageStash.get_value(name) }}
  end

  defp log_level(s) when is_binary(s) do
    case String.downcase(s) do
      "debug" -> :debug
      "info" -> :info
      "warn" -> :warn
      "error" -> :error
      _ -> :debug
    end
  end

  defp purger(parent, period) do
    :timer.sleep(:timer.seconds(period))
    GenServer.call(parent, :purge)
    purger(parent, period)
  end

  defp stats_log(parent, name, period, level) when is_binary(name) and is_atom(level) do
    :timer.sleep(:timer.seconds(period))
    stats = GenServer.call(parent, :stats)
    log(level, "Queue #{name}: statistics: #{inspect(stats)}")
    stats_log(parent, name, period, level)
  end

  defp queue_poller(recv, qurl, region, access, secret, max_mess, wait, mangle_attrs) do
    # log(:debug, "Polling queue #{qurl}")
    case (ExAws.SQS.receive_message(qurl |> qname, wait_time_seconds: wait, max_number_of_messages: max_mess) |>
      ExAws.request(region: region, access_key_id: access, secret_access_key: secret) ) do
      {:ok, %{body: %{messages: mess}, status_code: 200}} ->

        m = Enum.map(mess, fn %{body: b, message_id: mid, receipt_handle: rh} ->
          { rh, mid, b } end) |>
          Enum.reject(fn {r, _, _} -> r == "" end) |>
          Enum.map(fn {r, id, msg} -> {r, id, Poison.decode(msg)} end) |>
          Enum.reject(fn {_r, _i, {:error, _e}} -> true
                         {_r, _i, {:ok, _}} -> false end) |>
          Enum.map(fn {r, i, {:ok, m}} -> {r, i, process_message(m, mangle_attrs)} end) |>
          Enum.reject(fn {_r, _i, m} -> m == nil end) |>
          Enum.map(fn {r, i, m} -> {"#{r},#{i}", m} end)

        send recv, {:add_messages, m}
      {:ok, %{body: b, status_code: sc}} ->
        log(:info, "Cannot receive message: status_code: #{inspect(sc)}, body: #{inspect(b)}")
        nil
      {:error, e} ->
        log(:info, "Cannot receive message: #{inspect(e)}")
        nil
    end
    queue_poller(recv, qurl, region, access, secret, max_mess, wait, mangle_attrs)
  end

  defp process_message(%{ "Message" => m,
                          "MessageAttributes" => %{
                            "Encoding" => %{ "Value" => enc} ,
                            "Nonce" => %{ "Value" => nonce},
                            "Node" => %{ "Value" => node},
                            "Timestamp" => %{ "Value" => ts}
                          }
                        }, _ma ) do
    %{ "body" => m,
       "attributes" => %{
         "nonce" => nonce,
         "node" => node,
         "encoding" => enc,
         "timestamp" => ts,
       }
    } |>
      Poison.encode!
  end

  defp process_message(%{ "Message" => m }, true) do
    # fake attributes, if not there
    %{ "body" => m,
       "attributes" => %{
         "nonce" => :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower),
         "node" => "unknown",
         "encoding" => "raw",
         "timestamp" => DateTime.utc_now |> DateTime.to_iso8601,
       }
    } |>
      Poison.encode!
  end

  defp process_message(e, _ma) do
    log(:warn, "Processing bad message: #{inspect(e, pretty: true)}")
    nil
  end

  defp create_topic(topic, region, access, secret) do
    case ExAws.SNS.create_topic(topic) |> ExAws.request(region: region, access_key_id: access, secret_access_key: secret) do
      {:ok, %{body: %{topic_arn: tarn} = b, status_code: 200}} ->
        log(:debug, "CreateTopic response in #{region}: #{inspect(b, pretty: true)}")
        {:ok, tarn}
      {:error, {:http_error, err, _msg}} ->
        {:error, "Unable to create topic in #{region}: #{err}"}
    end
  end

  defp create_and_subscribe_queue({:error, e}, _q, _r, _a, _s, _o), do: {:error, e}
  defp create_and_subscribe_queue({:ok, tarn}, qname, region, access, secret, opts) do
    r = ExAws.SQS.create_queue(qname) |>
      ExAws.request(region: region, access_key_id: access, secret_access_key: secret) |>
      create_queue_result |>
      subscribe_queue_result(tarn, region, access, secret, opts)
    log(:debug, "Result of queue creation = #{inspect(r)}")
    r
  end

  defp create_queue_result({:error, e}), do: {:error, e}
  defp create_queue_result({:ok, %{body: %{queue_url: qurl}, status_code: 200}}), do: {:ok, qurl}

  defp qname(nil), do: nil
  defp qname(qstr) do
    URI.parse(qstr).path |> String.replace_prefix("/", "")
  end

  defp queue_arn(qurl, region, access, secret) when is_binary(qurl) do
    case (ExAws.SQS.get_queue_attributes(qurl |> qname, [:QueueArn]) |>
      ExAws.request(region: region, access_key_id: access, secret_access_key: secret) ) do
      {:ok, %{body: %{attributes: %{queue_arn: qarn}}, status_code: 200}} ->
        {:ok, qarn}
      {:ok, %{body: b, status_code: sc}} ->
        {:error, "Queue Attribute failed: #{sc}: #{inspect(b)}"}
      {:error, e} ->
        {:error, "Queue Attributes failed: #{inspect(e)}"}
    end
  end

  defp subscribe_queue_result({:error, e}, _tarn, _region, _access, _secret, _o), do: {:error, e}
  defp subscribe_queue_result({:ok, qurl}, tarn, region, access, secret, opts) do
    case queue_arn(qurl, region, access, secret) |>
      set_queue_policy(qurl, tarn, region, access, secret, opts) |>
      subscribe(tarn) |>
      do_subscribe(region, access, secret) do
      {:ok, %{status_code: 200, body: b}} ->
        _sarn = xpath(b, ~x"//SubscribeResponse/SubscribeResult/SubscriptionArn/text()"s) |> qname
        {:ok, qurl}
      {:ok, %{body: b, status_code: sc}} ->
        {:error, "Subscription error #{sc}: #{b}"}
      {:error, e} ->
        {:error, "Subscription error: #{inspect(e)}"}
    end
  end

  defp make_queue_policy(qarn, tarn) do
    Poison.encode!(
      %{
        "Version" => "2012-10-17",
        "Id" => qarn <> "/SNSExQueuePolicy",
        "Statement" => [
          %{
            "Effect" => "Allow",
            "Principal" => %{
              "AWS" => "*"
            },
            "Action" => "SQS:SendMessage",
            "Resource" => qarn,
            "Condition" => %{
              "ArnEquals" => %{
                "aws:SourceArn" => tarn
              }
            }
          }
        ]
      }
    )
  end

  defp sns_request(action, params) do
    %ExAws.Operation.Query{
      path: "/",
      params: params |> Map.put("Action", action),
      service: :sns
    }
  end

  defp subscribe({:error, e}, _tarn), do: {:error, e}

  defp subscribe({:ok, qarn}, tarn) do
    sns_request("Subscribe",
      %{ "Endpoint" => qarn,
         "Protocol" => "sqs",
         "TopicArn" => tarn
      })
  end

  defp do_subscribe(op, region, access, secret), do: ExAws.request(op, region: region, access_key_id: access,
    secret_access_key: secret)

  defp set_queue_policy({:error, e}, _, _, _, _, _, _), do: {:error, e}
  defp set_queue_policy({:ok, qarn}, qurl, tarn, region, access, secret, opts) do
    ttl = Keyword.get(opts, :ttl, 3600)
    case (ExAws.SQS.set_queue_attributes(qurl |> qname, [{ :message_retention_period, ttl },
                                                { :policy, make_queue_policy(qarn, tarn)}]) |>
      ExAws.request(region: region, access_key_id: access, secret_access_key: secret) ) do
      {:ok, %{status_code: 200}} ->
        {:ok, qarn}
      {:ok, %{body: b, status_code: sc}} ->
        {:error, "Queue Attribute set failed: #{sc}: #{b}"}
      {:error, e} ->
        {:error, "Queue Attribute set failed: #{inspect(e)}"}
   end
  end

  def start_link(cf = %{ "name" => n}, qa) when is_binary(n) do
    sname = (to_string(__MODULE__) <> "." <> n) |> String.to_atom
    GenServer.start_link(__MODULE__, [cf,sname,qa], name: sname)
  end

  def publish(s, obj) when is_binary(obj) do
    GenServer.call s, {:put, obj}
  end

  defp add_message_if_missing(b, {id, m}) do
    [_rhid, mid | _ ] = String.split(id, ",", parts: 2)
    case Enum.find(b, fn {bid, _dt, _m} -> [_brhid, bmid | _] = String.split(bid, ",", parts: 2); mid == bmid end) do
      nil -> {b ++ [{id, DateTime.utc_now |> DateTime.to_unix, m}], nil}
      {bid, _dt, _m} ->
        [brhid, bmid | _] = String.split(bid, ",", parts: 2)
        {b, {brhid, bmid}}
    end
  end

  def purge_buffer(b, timeout, opts \\ []) do
    now = Keyword.get(opts, :ts, DateTime.utc_now |> DateTime.to_unix)
    olen = Enum.count(b)
    nb = Enum.reject(b, fn {_bid, ts, _m} -> ts < now - timeout end)
    nlen = Enum.count(nb)
    log(:debug, "Buffer purge complete: #{nlen} items remain from #{olen}")
    nb
  end

  defp add_if_missing(ml, st) when is_list(ml) do
    b = st[:buffer]
    {b, dupl} = List.foldl(ml, {b,[]}, fn (m, {b, dupl}) -> {b, dup} = add_message_if_missing(b, m); {b, [dup | dupl]} end)
    # drop duplicate messages from AWS, so we don't see them again
    case Enum.reject(dupl, fn x -> x == nil end) do
      [] -> nil
      dl ->
        handles = Enum.map(dl, fn {r, i} -> %{ id: i, receipt_handle: r } end)
        case ExAws.SQS.delete_message_batch(st[:qurl] |> qname, handles) |>
          ExAws.request(region: st[:region], access_key_id: st[:access], secret_access_key: st[:secret]) do
          {:ok, %{body: %{successes: successes}, status_code: 200}} ->
            if Enum.count(successes) < Enum.count(handles) do
              log(:debug, "Result of delete duplicate message batch of #{Enum.count(handles)} in #{st[:region]}: #{Enum.count(successes)}")
            end
          {:ok, b = %{status_code: sc}} ->
            log(:info, "Cannot delete duplicate messages #{inspect(handles)}: status_code: #{inspect(sc)}: #{inspect(b)}")
          {:error, e} ->
            log(:info, "Cannot delete duplicate messages #{inspect(handles)}: #{inspect(e)}")
        end
    end
    b
  end

  def handle_call({:put, obj}, _from, st) when is_binary(obj) do
    {:ok, m} = Poison.decode(obj)
    mattrs = [
      %{ name: "Timestamp", data_type: "String",
         value: { "String", m["attributes"]["timestamp"]}},
      %{ name: "Nonce", data_type: "String",
         value: { "String", m["attributes"]["nonce"] } },
      %{ name: "Node", data_type: "String",
         value: { "String", m["attributes"]["node"] } },
      %{ name: "Encoding", data_type: "String",
         value: { "String", m["attributes"]["encoding"] } }
    ]
    r = case SNSFix.publish(m["body"], topic_arn: st[:tarn], message_attributes: mattrs) |>
      ExAws.request(region: st[:region], access_key_id: st[:access], secret_access_key: st[:secret]) do
        {:ok, %{body: b, status_code: 200}} ->
          # log(:debug, "Publish to AWS #{st[:region]} OK: #{inspect(b)}")
          b
        {:ok, resp = %{status_code: sc}} ->
          log(:debug, "Publish to AWS #{st[:region]} FAIL: #{inspect(sc)}")
          resp
        e ->
          log(:warn, "Publish to AWS #{st[:region]} HTTP FAIL: #{inspect(e)}")
          e
      end
    {:reply, r, st}
  end

  def handle_call({:get, num, _time}, _from, st) do
    buffer = Map.get(st, :buffer)
    r = Enum.take(buffer, num) |> Enum.map(fn {id, _dt, m} -> {id, m} end)
    buffer = Enum.drop(buffer, Enum.count(r))
    st = Map.put(st, :buffer, buffer)
    {:reply, r, st}
  end

  def handle_call(:purge, _from, st) do
    {:reply, nil, Map.put(st, :buffer, purge_buffer(st[:buffer], st[:buffer_timeout]))}
  end

  def handle_call(:stats, _from, st) do
    nun = Enum.uniq_by(st[:buffer], fn {_id, _dt, m} -> m end) |> Enum.count
    g = Enum.group_by(st[:buffer], fn {_id, _dt, m} -> m end) |>
      Enum.reject(fn {_k, v} -> Enum.count(v) < 2 end) |>
      Enum.sort(fn {_k1,v1}, {_k2, v2} -> Enum.count(v1) > Enum.count(v2) end) |>
      Enum.take(2) |>
      Enum.into(%{})
    {:reply, "Number of messages in buffer: #{Enum.count(st[:buffer])}. Number unique: #{nun}. Top clashes #{inspect(g)}", st}
  end

  def handle_call({:ack, []}, _from, st), do: {:reply, :ok, st}
  def handle_call({:ack, qidlist}, _from, st) do
    handles = Enum.map(qidlist, fn m -> String.split(m, ",") end) |>
      Enum.map(fn [r,i|_] -> %{ id: i, receipt_handle: r } end)
    case ExAws.SQS.delete_message_batch(st[:qurl] |> qname, handles) |>
      ExAws.request(region: st[:region], access_key_id: st[:access], secret_access_key: st[:secret]) do
      {:ok, %{body: %{successes: successes}, status_code: 200}} ->
        if Enum.count(successes) < Enum.count(handles) do
          log(:debug, "Result of delete message batch of #{Enum.count(handles)} in #{st[:region]}: #{Enum.count(successes)}")
        end
        {:reply, :ok, st}
      {:ok, b = %{status_code: sc}} ->
        log(:info, "Cannot delete messages #{inspect(handles)}: status_code: #{inspect(sc)}: #{inspect(b)}")
        {:reply, :error, st}
      {:error, e} ->
        log(:info, "Cannot delete messages #{inspect(handles)}: #{inspect(e)}")
        {:reply, :error, st}
      end
  end

  def handle_call({:nack, qidlist}, _from, st) do
    handles = Enum.map(qidlist, fn m -> String.split(m, ",") end) |>
      Enum.map(fn [r,i|_] -> %{ id: i, receipt_handle: r, visibility_timeout: 0 } end)
    case ExAws.SQS.change_message_visibility_batch(st[:qurl] |> qname, handles) |>
      ExAws.request(region: st[:region], access_key_id: st[:access], secret_access_key: st[:secret]) do
      {:ok, %{body: b, status_code: 200}} ->
        log(:debug, "Result of change visibility batch #{inspect(handles)} in #{st[:region]}: #{inspect(b)}")
        {:reply, :ok, st}
      {:ok, b = %{status_code: sc}} ->
        log(:info, "Cannot change visibility for messages #{inspect(handles)}: status_code: #{inspect(sc)}: #{inspect(b)}")
        {:reply, :error, st}
      {:error, e} ->
        log(:info, "Cannot change visibility for messages #{inspect(handles)}: #{inspect(e)}")
        {:reply, :error, st}
      end
    {:reply, :ok, st}
  end

  def handle_info({:add_messages, msgs}, st) do
    st = Map.put(st, :buffer, add_if_missing(msgs, st))
    {:noreply, st}
  end

  def terminate(_reason, st) do
    ExQueue.MessageStash.save_value(st[:name], st[:buffer])
  end
end

defmodule SNSFix do
  # fix for MessageAttribute support in ExAWS
  import ExAws.Utils, only: [camelize_keys: 1]

  def publish(message, opts) do
    opts = opts |> Map.new

    message_attrs = opts
    |> Map.get(:message_attributes, [])
    |> build_message_attributes

    params = opts
    |> Map.drop([:message_attributes])
    |> camelize_keys
    |> Map.put("Message", message)
    |> Map.merge(message_attrs)

    request("Publish", params)
  end

  defp build_message_attributes(attrs) do
    attrs
    |> Stream.with_index(1) # AWS MA's need to start at 1, not 0
    |> Enum.reduce(%{}, &build_message_attribute/2)
  end

  def build_message_attribute({%{name: name, data_type: data_type, value: {value_type, value}}, i}, params) do
    param_root = "MessageAttributes.entry.#{i}"
    value_type = value_type |> String.capitalize

    params
    |> Map.put(param_root <> ".Name", name)
    |> Map.put(param_root <> ".Value.#{value_type}Value", value)
    |> Map.put(param_root <> ".Value.DataType", data_type)
  end

  defp request(action, params) do
    %ExAws.Operation.Query{
      path: "/",
      params: params |> Map.put("Action", action),
      service: :sns
    }
  end
end
