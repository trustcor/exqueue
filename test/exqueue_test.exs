defmodule ExqueueTest do
  use ExUnit.Case

  setup do
    {:ok, qa} = Agent.start_link(fn -> %{ queues: %{}, node: "testnode",
                                          max_age: 86400} end)
    Application.start(:elmdb)
    path = Briefly.create!(directory: true)
    config = %{
      "name" => "testlocal",
      "path" => path
    }
    {:ok, pid} = ExQueue.Local.start_link([config, qa])
    on_exit fn ->
      Application.stop(:elmdb)
      File.rm_rf!(Path.dirname(path))
    end
    {:ok, q: pid, qa: qa}
  end

  test "test agent registry", %{qa: qa} do
    assert Agent.get(qa, fn m -> Map.get(m, :queues, %{}) |> Map.keys end) == [ "testlocal" ]
  end

  test "test local publish", %{qa: qa} do
    body = "TestMessage1"
    s = Agent.get(qa, fn m -> get_in(m, [:queues, "testlocal", :server]) end)
    assert ExQueue.Local.publish(s, body) == :ok
    t = GenServer.call s, {:get, 1, 0}
    [{qid, ^body}] = t
    assert byte_size(qid) == 16
  end

  test "test local publish and ack", %{qa: qa} do
    body = "TestMessage1"
    s = Agent.get(qa, fn m -> get_in(m, [:queues, "testlocal", :server]) end)
    assert ExQueue.Local.publish(s, body) == :ok
    t = GenServer.call s, {:get, 1, 0}
    assert Enum.count(t) == 1
    [{qid, ^body}] = t
    assert byte_size(qid) == 16
    assert GenServer.call(s, {:ack, [qid]}) == [{qid, :ok}]
    t = GenServer.call s, {:get, 1, 0}
    assert Enum.count(t) == 0
  end
end
