defmodule ExQueue.LocalSup do
  use Supervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, [args])
  end

  def init([[cf, qa]]) do
    children = Enum.map(cf, fn l -> worker(ExQueue.Local, [[l, qa]], id: Map.get(l, "name")) end)

    supervise(children, strategy: :one_for_one)
  end
end

defmodule ExQueue.Local do
  import Logger, only: [log: 2]

  use GenServer

  defp initialize_queue(env, n = :default, q, opts) when is_list(opts) do
    expire = Keyword.get(opts, :expire, 3600) # default 1 hour expiry time
    fexpire = Keyword.get(opts, :flight_expire, 60) # default 60 seconds to acknowledge receipt of entry
    {:ok, txn} = :elmdb.txn_begin(env)
    :elmdb.txn_put_new(txn, q, "max_ndx", uint64(0)) # only write if there is no such entry
    # {:ok, max_ndx} = :elmdb.txn_get(txn, q, "max_ndx")
    # log(:debug, "max_ndx = #{:binary.decode_unsigned(max_ndx)}")
    :elmdb.txn_put(txn, q, "expire", uint64(expire)) # overwrite any existing value
    :elmdb.txn_put(txn, q, "flight_expire", uint64(fexpire))
    :ok = :elmdb.txn_commit(txn)
    {n ,q}
  end

  defp initialize_queue(_env, n, q, _opts), do: {n, q} # by default, do nothing

  defp make_dbs(env, qopts) when is_list(qopts) do
    # we can't do dupsorted time databases, because
    # elmdb at time of writing doesn't support the delete <key>,<val>
    # API call which LMDB does.
    #
    # So we use the timestamp as a prefix and append the
    # qid as a discriminator. Makes the DB bigger, but
    # we're sure each key is valid, while still preserving
    # time sequence order
    qs = [{:default, ""}, {:queue, "qids"}, {:byorder, "order"},
                  {:bytime, "ts"}, {:byftime, "ts_in_flight"}]
    r =
      Enum.map(qs,
        fn {n, k} -> {n, :elmdb.db_open(env, k, [:create])} end) |>
      Enum.map(fn
        {n, {:ok, q}} -> {n, q}
        {n, e} ->
          log(:error, "Unable to create db #{inspect(n)}: #{inspect(e)}")
          {n, nil}
      end) |>
      Enum.reject(fn {_n, q} -> q == nil end) |>
      Enum.map(fn {n, q} -> initialize_queue(env, n, q, qopts) end)
    case Enum.count(r) == Enum.count(qs)  do
      true -> r
      false -> nil
    end
  end

  defp get_option(x = %{}) do
    # return the first value pair of the map as an {atom, value} tuple
    case Map.keys(x) do
      [ k | _ ] when is_binary(k) -> { String.downcase(k) |> String.to_atom, Map.get(x, k) }
      _ -> nil
    end
  end

  defp get_option(_), do: nil

  def init([cf,sname,qa]) do
    name = Map.get(cf, "name")
    path = Map.get(cf, "path") |> String.to_charlist
    qopts = Enum.map(Map.get(cf, "options", []),
      fn x -> get_option(x) end) |>
      Enum.reject(fn x -> x == nil end)
    {env, dbs} = case :elmdb.env_open(path, [{:max_dbs, 5}]) do
                 {:ok, env} -> case make_dbs(env, qopts) do
                                 nil ->
                                   :elmdb.env_close(env)
                                   {nil, nil}
                                 dbs when is_list(dbs) ->
                                   {env, Enum.into(dbs, %{})}
                               end
                 e ->
                   log(:error, "Unable to open path #{path}: #{inspect(e)}")
                   {nil, nil}
               end
    Enum.each(dbs, fn d -> Agent.update(qa, fn m -> put_in(m,[:queues, name], %{ server: sname, env: env, db: d}) end) end)
    spawn_link(fn -> expiry_thread(path, qopts) end)
    spawn_link(fn -> flight_thread(path, qopts) end)
    {:ok, %{ env: env, db: dbs }}
  end

  defp expiry_thread(path, qopts) do
    # log(:info, "Performing expiry thread on #{inspect(path)}")
    {:ok, env} = :elmdb.env_open(path, [{:max_dbs, 5}])
    dbs = (make_dbs(env, qopts) |> Enum.into(%{}))
    search_time(env, %{db: dbs}, :bytime, &ack_qid/3)
    :elmdb.env_close(env)

    :timer.sleep(:timer.seconds(Keyword.get(qopts, :expire_cycle, 30)))
    expiry_thread(path, qopts)
  end

  defp flight_thread(path, qopts) do
    # log(:info, "Performing flight thread on #{inspect(path)}")
    {:ok, env} = :elmdb.env_open(path, [{:max_dbs, 5}])
    dbs = (make_dbs(env, qopts) |> Enum.into(%{}))
    search_time(env, %{db: dbs}, :byftime, &nack_qid/3)
    :elmdb.env_close(env)

    :timer.sleep(:timer.seconds(Keyword.get(qopts, :flight_cycle, 30)))
    flight_thread(path, qopts)
  end

  def start_link([cf = %{ "name" => n}, qa]) when is_binary(n) do
    sname = (to_string(__MODULE__) <> "." <> n) |> String.to_atom
    GenServer.start_link(__MODULE__, [cf,sname,qa], name: sname)
  end

  defp uint64(x) when is_integer(x) and x >= 0 do
    r = << 0 :: size(64) >> <> :binary.encode_unsigned(x)
    :binary.part(r, {byte_size(r), -8})
  end

  defp inc({:ok, x}), do: inc_u(x)

  defp inc_u(:not_found), do: :not_found
  defp inc_u(x) do
    u = :binary.decode_unsigned(x)
    uint64(u + 1)
  end

  def mark_in_flight(txn, q, qid, fe, e, order, obj) do
    fexp = uint64((DateTime.utc_now |> DateTime.to_unix) + fe)
    # mark this record as now being 'in flight'
    :ok = :elmdb.txn_put(txn, q[:db][:byftime], fexp <> qid, qid)
    nexp = uint64(e)
    nrec = nexp <> fexp <> uint64(order) <> obj
    # log(:info, "Replacing #{inspect(qid)} -> #{inspect(nrec)} [#{inspect(nexp)}/#{inspect(fexp)}/#{inspect(order)}]")
    :ok = :elmdb.txn_put(txn, q[:db][:queue], qid, nrec)
  end

  def next_valid_record(txn, q, fe, num, {size, _k, _val, cur}) do
    case :elmdb.txn_cursor_get(cur, :next) do
      {:ok, k, v} ->
        t = :elmdb.txn_get(txn, q[:db][:queue], v)
        {:ok, << e :: size(64), f :: size(64), order :: size(64), o :: binary >>} = t
        case f do
          0 ->
            if size < num, do: mark_in_flight(txn, q, v, fe, e, order, o)
            {size + 1, k, {v, o}, cur}
          _fr ->
            # log(:info, "#{inspect(k)} is in flight [#{inspect(fr)}] - skipping")
            {size, k, nil, cur} # skip this record - it's already in flight
        end
      :not_found ->
        {size, :not_found, nil, cur}
    end
  end

  defp ack_qid_helper({:ok, << e :: size(64), f :: size(64), order :: size(64), _o :: binary >>}, txn, q, qid) do
    # log(:info, "ACK-ing [#{inspect(qid)}/#{inspect(t)}]: #{e}/#{f}/#{order}/#{inspect(o)}")
    r = :elmdb.txn_delete(txn, q[:db][:byorder], uint64(order))
    if r == :not_found, do: log(:info, "ack #{inspect(qid)}: order delete #{inspect(order)} = #{inspect(r)}")
    r = :elmdb.txn_delete(txn, q[:db][:bytime], uint64(e) <> qid)
    if r == :not_found, do: log(:info, "ack #{inspect(qid)}: time delete #{inspect(e)}= #{inspect(r)}")
    if f > 0 do
      r = :elmdb.txn_delete(txn, q[:db][:byftime], uint64(f) <> qid)
      if r == :not_found, do: log(:info, "ack #{inspect(qid)}: ftime delete #{inspect(f)} = #{inspect(r)}")
    end
    r = :elmdb.txn_delete(txn, q[:db][:queue], qid)
    if r == :not_found, do: log(:info, "ack #{inspect(qid)}: qid delete = #{inspect(r)}")
    {qid, :ok}
  end

  defp ack_qid_helper(:not_found, _txn, _q, qid) do
    {qid, :not_found}
  end

  def ack_qid(txn, q, qid) do
    ack_qid_helper(:elmdb.txn_get(txn, q[:db][:queue], qid), txn, q, qid)
  end

  defp nack_qid_helper({:ok, << e :: size(64), f :: size(64), order :: size(64), o :: binary >>}, txn, q, qid) do
    # log(:info, "NACK-ing [#{inspect(qid)}/#{inspect(t)}]: #{e}/#{f}/#{order}/#{inspect(o)}")
    if f > 0 do
      r = :elmdb.txn_delete(txn, q[:db][:byftime], uint64(f) <> qid)
      if r == :not_found, do: log(:info, "nack #{inspect(qid)}: ftime delete #{inspect(f)} = #{inspect(r)}")
    end
    t = uint64(e) <> uint64(0) <> uint64(order) <> o
    r = :elmdb.txn_put(txn, q[:db][:queue], qid, t)
    if r == :not_found, do: log(:info, "nack #{inspect(qid)}: qid replace = #{inspect(r)}")
    {qid, :ok}
  end

  defp nack_qid_helper(:not_found, _txn, _q, qid) do
    {qid, :not_found}
  end

  def nack_qid(txn, q, qid) do
    nack_qid_helper(:elmdb.txn_get(txn, q[:db][:queue], qid), txn, q, qid)
  end

  def search_time(env, q, db, f, ts \\ nil) do
    d = q[:db][db]
    {:ok, txn} = :elmdb.txn_begin(env)
    ts = case ts do
           nil -> DateTime.utc_now |> DateTime.to_unix
           _ -> ts
         end
    {:ok, cur} = :elmdb.txn_cursor_open(txn, d)
    iv = :elmdb.txn_cursor_get(cur, :first) # start from oldest, work up
    r = Stream.iterate({iv, cur}, fn {_v, cur} -> {:elmdb.txn_cursor_get(cur, :next), cur} end) |>
      Enum.take_while(fn
        {:not_found, _cur} -> false
        {{:ok, << st :: size(64),  _q :: binary >>, _v} , _cur} -> st <= ts
      end)
    _stamps = Enum.map(r, fn {{:ok, << v :: size(64),  _q :: binary >>, qid}, _c} -> {v, f.(txn, q, qid)} end)
    # {stless,stmore} = Enum.partition(stamps, fn {st,_v} -> st <= ts end)
    # log(:info, "Result from ts #{inspect(ts)} = #{Enum.count(stamps)}/#{Enum.count(stless)}/#{Enum.count(stmore)}")
    :ok = :elmdb.txn_commit(txn)
  end

  def publish(s, obj) when is_binary(obj) do
    GenServer.call s, {:put, obj}
  end

  # handlers below here

  def handle_call({:put, obj}, _from, q) when is_binary(obj) do
    {:ok, txn} = :elmdb.txn_begin(q[:env])
    max_ndx = inc(:elmdb.txn_get(txn, q[:db][:default], "max_ndx"))

    qid = :crypto.strong_rand_bytes(16) # unique id
    {:ok, rexp} = :elmdb.txn_get(txn, q[:db][:default], "expire")
    exp = :binary.decode_unsigned(rexp)
    etime = (DateTime.utc_now |> DateTime.to_unix) + exp
    o = uint64(etime) <> uint64(0) <> max_ndx <> obj
    :ok = :elmdb.txn_put(txn, q[:db][:queue], qid, o) # key, value store
    :ok = :elmdb.txn_put(txn, q[:db][:byorder], max_ndx, qid) # order based queue
    :ok = :elmdb.txn_put(txn, q[:db][:bytime], uint64(etime) <> qid, qid) # time based queue
    :ok = :elmdb.txn_put(txn, q[:db][:default], "max_ndx", max_ndx)
    :ok = :elmdb.txn_commit(txn)

    {:reply, :ok, q}
  end

  def handle_call({:get, num, _time}, _from, q) do
    {:ok, txn} = :elmdb.txn_begin(q[:env])
    {:ok, fexp} = :elmdb.txn_get(txn, q[:db][:default], "flight_expire")
    fe = :binary.decode_unsigned(fexp)
    {:ok, cur} = :elmdb.txn_cursor_open(txn, q[:db][:byorder])
    r = case :elmdb.txn_cursor_get(cur, :first) do
          :not_found -> []
          {:ok, k, v} ->
            iv = case :elmdb.txn_get(txn, q[:db][:queue], v) do
                   {:ok, << e :: size(64), f :: size(64), order :: size(64), o :: binary >>} when (f == 0) ->
                     mark_in_flight(txn, q, v, fe, e, order, o)
                     {1, k, {v, o}, cur}
                   {:ok, _t} -> {0, k, nil, cur}
                   :not_found -> {0, k, nil, cur}
                 end
            Stream.iterate(iv,
              fn s -> next_valid_record(txn, q, fe, num, s) end) |>
            Enum.take_while(fn {size, ord, _obj, _cur} -> ord != :not_found and size <= num end)
        end |>
      Enum.reject(fn {_, _, x, _} -> x == nil end) |>
      Enum.map(fn {_, _, x, _} -> x end)

    :ok = :elmdb.txn_commit(txn)
    {:reply, r, q}
  end

  def handle_call({:ack, qids}, _from, q) do
    {:ok, txn} = :elmdb.txn_begin(q[:env])
    r = Enum.map(qids, fn qid -> ack_qid(txn, q, qid) end) |> Enum.reject(fn x -> x == nil end)
    :ok = :elmdb.txn_commit(txn)
    {:reply, r, q}
  end

  def handle_call({:nack, qids}, _from, q) do
    {:ok, txn} = :elmdb.txn_begin(q[:env])
    r = Enum.map(qids, fn qid -> nack_qid(txn, q, qid) end) |> Enum.reject(fn x -> x == nil end)
    :ok = :elmdb.txn_commit(txn)
    {:reply, r, q}
  end

  def handle_call({:search_expire, ts}, _from, q) do
    r = search_time(q[:env], q, :bytime, &ack_qid/3, ts)
    {:reply, r, q}
  end

end
