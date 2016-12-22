defmodule LocalDBTest do
  use ExUnit.Case
  use ExQueue.LocalDB

  require Amnesia.Helper
  
  import Logger, only: [log: 2]
  
  setup do
    Amnesia.Test.start
    case ExQueue.LocalDB.create(memory: [node]) do
      e -> log(:info, "Database creation: #{inspect(e)}")
    end
    Amnesia.Table.clear(ExQueue.LocalDB.Queues)
    Amnesia.Table.clear(ExQueue.LocalDB.Messages)
    on_exit fn ->
      Amnesia.Test.stop
    end
  end
  
  test "Check Empty Database" do
    Amnesia.Table.clear(ExQueue.LocalDB.Queues)
    Amnesia.transaction! do
      assert ExQueue.LocalDB.Queues.keys() == []
    end
  end

  test "Check Single Row in Queues" do
    Amnesia.transaction! do
      %ExQueue.LocalDB.Queues{name: "foo", expire: 10, fexpire: 5} |> ExQueue.LocalDB.Queues.write!
    end
    Amnesia.transaction! do
      assert ExQueue.LocalDB.Queues.keys() == [1]
    end
  end

  test "Add a Queue, then add a message" do
    Amnesia.transaction! do
      %ExQueue.LocalDB.Queues{name: "foo", expire: 10, fexpire: 5} |> ExQueue.LocalDB.Queues.write!
    end
    Amnesia.transaction! do
      [qid] = (ExQueue.LocalDB.Queues.where name == "foo", select: queue_id) |> Amnesia.Selection.values
      assert qid == 1
      %ExQueue.LocalDB.Messages{queue_id: qid, body: "test body"} |> ExQueue.LocalDB.Messages.write!
      assert ExQueue.LocalDB.Messages.last().msg_id == 1
    end
  end

  test "test add to bad queue" do
    assert ExQueue.Local.add_message("foo", "something") == {:error, :no_such_queue}
  end

  test "test add to good queue" do
    assert ExQueue.Local.initialize_queue("foo") == 1
    assert ExQueue.Local.add_message("foo", "something").msg_id == 1
  end

  test "make attributes orthogonal" do
    exp = Enum.sort([
      ts: 1451744116,
      node: "mynode",
      encoding: "raw",
      nonce: "0123456"
    ])
    val = %{
      "Encoding" => "raw",
      "Nonce" => "0123456",
      "Node" => "mynode",
      "Timestamp" => "2016-01-02T14:15:16Z"
    }
    
    assert ExQueue.Local.make_attrs(val) == exp
  end
  
  test "test add to two queues" do
    assert ExQueue.Local.initialize_queue("foo") == 1
    assert ExQueue.Local.initialize_queue("bar") == 2
    at = [ts: (DateTime.utc_now |> DateTime.to_unix), node: "mynode", msg_id: "fake-uuid", nonce: "fake-nonce", encoding: "raw"]
    assert ExQueue.Local.add_message("foo", "something", at) |> Map.take([:queue_id, :msg_id]) == %{queue_id: 1, msg_id: 1}
    assert ExQueue.Local.add_message("bar", "something else", at) |> Map.take([:queue_id, :msg_id]) == %{queue_id: 2, msg_id: 2}
    assert ExQueue.Local.add_message("bar", "something three", at) |> Map.take([:queue_id, :msg_id]) == %{queue_id: 2, msg_id: 3}
  end

  test "queue empty check" do
    assert ExQueue.Local.queue_empty("foo") == {:error, :no_such_queue}
    ExQueue.Local.initialize_queue("foo")
    assert ExQueue.Local.queue_empty("foo") == true
    at = [ts: (DateTime.utc_now |> DateTime.to_unix), node: "mynode", nonce: "fake-nonce", encoding: "raw"]
    assert ExQueue.Local.add_message("foo", "something", at) |> Map.take([:queue_id, :msg_id]) == %{queue_id: 1, msg_id: 1}
    assert ExQueue.Local.queue_empty("foo") == false
  end

  test "add and remove queue item" do
    ExQueue.Local.initialize_queue("foo")
    at = ExQueue.Local.make_attrs(%{
	  "Encoding" => "raw",
	  "Nonce" => "0123456",
	  "Node" => "mynode",
	  "Timestamp" => "2016-01-02T14:15:16Z"
				  })
    ExQueue.Local.add_message("foo", "something", at)
    ms = ExQueue.Local.get_messages("foo", 1)
    assert Enum.map(ms, fn m -> m.body end) == [ "something" ]
    ms = ExQueue.Local.get_messages("foo", 1)
    assert ms == []    
  end
end
