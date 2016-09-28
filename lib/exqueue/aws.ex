defmodule ExQueue.AwsSup do
  use Supervisor

  def init(_args) do
    {:ok, []}
  end

  def start_link([cf, qa]) do
    c = [worker(ExQueue.Aws, [], restart: :transient)]
    {:ok, sup_pid} = Supervisor.start_link(c, strategy: :simple_one_for_one)
    Agent.update(qa, fn m -> Map.put(m, :aws, sup_pid) end)
    add_config(cf, qa)
    {:ok, sup_pid}
  end

  def cstart(sup_pid, cf, qa) do
    {:ok, _cpid} = Supervisor.start_child(sup_pid, [cf, qa])
  end

  def add_config(cf, qa) do
    sup_pid = Agent.get(qa, fn m -> Map.get(m, :aws) end)
    Enum.map(cf, fn l -> cstart(sup_pid, l, qa) end)
  end
end

defmodule ExQueue.Aws do
  import Logger, only: [log: 2]
  import SweetXml

  use GenServer

  def init([cf,sname,qa]) do
    name = Map.get(cf, "name")
    topic = Map.get(cf, "topic", name)
    region = Map.get(cf, "region", "us-east-1")
    access = get_in(cf, ["access_key_id"])
    secret = get_in(cf, ["secret_access_key"])
    wait = Map.get(cf, "wait", 20)
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

    {:ok, %{ag: qa, region: region, access: access, secret: secret, tarn: tarn, name: name,
            qurl: qurl,
            buffer:  ExQueue.MessageStash.get_value(name) }}
  end

  defp queue_poller(recv, qurl, region, access, secret, max_mess, wait, mangle_attrs) do
    case (ExAws.SQS.receive_message(qurl, wait_time_seconds: wait, max_number_of_messages: max_mess) |>
      ExAws.request(region: region, access_key_id: access, secret_access_key: secret) ) do
      {:ok, %{body: b, status_code: 200}} ->
        m = Enum.map(1..max_mess, fn i ->
          {
            xpath(b, ~x"//ReceiveMessageResponse/ReceiveMessageResult/Message[#{i}]/ReceiptHandle/text()"s),
            xpath(b, ~x"//ReceiveMessageResponse/ReceiveMessageResult/Message[#{i}]/MessageId/text()"s),
            xpath(b, ~x"//ReceiveMessageResponse/ReceiveMessageResult/Message[#{i}]/Body/text()"s)
          } end) |>
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
      {:ok, %{body: b, status_code: 200}} ->
        log(:debug, "CreateTopic #{inspect(topic)} in #{region}: OK")
        {:ok, xpath(b, ~x"//CreateTopicResponse/CreateTopicResult/TopicArn/text()"s)}
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
  defp create_queue_result({:ok, %{body: b, status_code: 200}}) do
    {:ok, xpath(b, ~x"//CreateQueueResponse/CreateQueueResult/QueueUrl/text()"s) |> qname}
  end

  defp qname(nil), do: nil
  defp qname(qstr) do
    URI.parse(qstr).path |> String.replace_prefix("/", "")
  end

  defp queue_arn(qurl, region, access, secret) when is_binary(qurl) do
    case (ExAws.SQS.get_queue_attributes(qurl, [:QueueArn]) |>
      ExAws.request(region: region, access_key_id: access, secret_access_key: secret) ) do
      {:ok, %{body: b, status_code: 200}} ->
        {:ok, b |>
          xpath(~x"//GetQueueAttributesResponse/GetQueueAttributesResult/Attribute[name=QueueArn]/Value/text()"s)}
      {:ok, %{body: b, status_code: sc}} ->
        {:error, "Queue Attribute failed: #{sc}: #{b}"}
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
    case (ExAws.SQS.set_queue_attributes(qurl, [{ :message_retention_period, ttl },
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
    case Enum.find(b, fn {bid, _m} -> id == bid end) do
      nil -> b ++ [{id, m}]
      _ -> b
    end
  end

  defp add_if_missing(b, ml) when is_list(ml) do
    List.foldl(ml, b, fn (m, b) -> add_message_if_missing(b, m) end)
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
    r = SNSFix.publish(m["body"], topic_arn: st[:tarn], message_attributes: mattrs) |>
      ExAws.request(region: st[:region], access_key_id: st[:access], secret_access_key: st[:secret])
    {:reply, r, st}
  end

  def handle_call({:get, num, _time}, _from, st) do
    buffer = Map.get(st, :buffer)
    r = Enum.take(buffer, num)
    buffer = Enum.drop(buffer, Enum.count(r))
    st = Map.put(st, :buffer, buffer)
    {:reply, r, st}
  end

  def handle_call({:ack, []}, _from, st), do: {:reply, :ok, st}
  def handle_call({:ack, qidlist}, _from, st) do
    handles = Enum.map(qidlist, fn m -> String.split(m, ",") end) |>
      Enum.map(fn [r,i|_] -> %{ id: i, receipt_handle: r } end)
    case ExAws.SQS.delete_message_batch(st[:qurl], handles) |>
      ExAws.request(region: st[:region], access_key_id: st[:access], secret_access_key: st[:secret]) do
      {:ok, %{body: b, status_code: 200}} ->
        log(:debug, "Result of delete message batch #{inspect(handles)} in #{st[:region]}: #{inspect(b)}")
        {:reply, :ok, st}
      {:ok, b = %{http_error: sc}} ->
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
    case ExAws.SQS.change_message_visibility_batch(st[:qurl], handles) |>
      ExAws.request(region: st[:region], access_key_id: st[:access], secret_access_key: st[:secret]) do
      {:ok, %{body: b, status_code: 200}} ->
        log(:debug, "Result of change visibility batch #{inspect(handles)} in #{st[:region]}: #{inspect(b)}")
        {:reply, :ok, st}
      {:ok, b = %{http_error: sc}} ->
        log(:info, "Cannot change visibility for messages #{inspect(handles)}: status_code: #{inspect(sc)}: #{inspect(b)}")
        {:reply, :error, st}
      {:error, e} ->
        log(:info, "Cannot change visibility for messages #{inspect(handles)}: #{inspect(e)}")
        {:reply, :error, st}
      end
    {:reply, :ok, st}
  end

  def handle_info({:add_messages, msgs}, st) do
    st = Map.put(st, :buffer, add_if_missing(st[:buffer], msgs))
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
