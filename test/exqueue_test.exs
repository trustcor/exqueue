defmodule ExqueueTest do
  use ExUnit.Case

  setup do
    Amnesia.Test.start
    ExQueue.LocalDB.create(memory: [node])
    Amnesia.Table.clear(ExQueue.LocalDB.Queues)
    Amnesia.Table.clear(ExQueue.LocalDB.Messages)
    on_exit fn ->
      Amnesia.Test.stop
    end
    
    {:ok, qa} = Agent.start_link(fn -> %{ queues: %{}, node: "testnode",
                                          max_age: 86400} end)
    config = %{
      "name" => "testlocal"
    }
    {:ok, pid} = ExQueue.Local.start_link(config, qa)
    
    {:ok, q: pid, qa: qa}
  end

  def make_message(text) do
    Poison.encode!(%{ "body" => text,
		      "attributes" => %{
			"timestamp" => Timex.format!(DateTime.utc_now(), "{ISO:Extended:Z}"),
			"nonce" => "my-nonce-val",
			"encoding" => "raw",
			"node" => "testnode",
			"messageid" => UUID.uuid4()
		      }})
  end
  
  test "test agent registry", %{qa: qa} do
    assert Agent.get(qa, fn m -> Map.get(m, :queues, %{}) |> Map.keys end) == [ "testlocal" ]
  end

  test "test local publish", %{qa: qa} do
    body = "TestMessage1"
    m = make_message(body)
    s = Agent.get(qa, fn m -> get_in(m, [:queues, "testlocal", :server]) end)
    assert ExQueue.Local.publish(s, m) == :ok
    t = GenServer.call s, {:get, 1, 0}
    [{qid, ^body}] = t
    assert byte_size(qid) == 16
  end

  test "test local publish and ack", %{qa: qa} do
    body = "TestMessage1"
    m = make_message(body)
    s = Agent.get(qa, fn m -> get_in(m, [:queues, "testlocal", :server]) end)
    assert ExQueue.Local.publish(s, m) == :ok
    t = GenServer.call s, {:get, 1, 0}
    assert Enum.count(t) == 1
    [{qid, ^body}] = t
    assert byte_size(qid) == 16
    assert GenServer.call(s, {:ack, [qid]}) == [{qid, :ok}]
    t = GenServer.call s, {:get, 1, 0}
    assert Enum.count(t) == 0
  end
end
