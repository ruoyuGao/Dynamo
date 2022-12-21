defmodule DataversionTest do
  use ExUnit.Case
  doctest Dynamo
  import Emulation, only: [spawn: 2, send: 2, mark_unfuzzable: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "can return different version of data, k test" do
    Emulation.init()
    # test config
    # Emulation.append_fuzzers([Fuzzers.delay(5.0),Fuzzers.drop(0.1)])
    # Emulation.append_fuzzers([Fuzzers.delay(10.0)])
    delay_time = 40
    Emulation.append_fuzzers([Fuzzers.delay(delay_time)])
    start_time = delay_time/2
    num_trials = 20
    get_interval = 10
    # system config
    w = r = n = 1
    # base_config_a = Dynamo.new_configuration([:b, :c], %{a: [240,360], b: [0,120], c: [120, 240]}, 1000, :client, w, r, n)
    base_config_a = Dynamo.new_configuration([:b, :c], %{a: [0,120], b: [120,240], c: [240, 360]}, 1000, w, r, n)
    base_config_b = Dynamo.new_configuration([:c, :a], %{a: [0,120], b: [120,240], c: [240, 360]}, 1000, w, r, n)
    base_config_c = Dynamo.new_configuration([:a, :b], %{a: [0,120], b: [120,240], c: [240, 360]}, 1000, w, r, n)

    # base_config_a = Dynamo.new_configuration([:b, :c], %{a: [0,120], b: [120,240], c: [240, 360]}, 10, :client, w, r, n)


    spawn(:a, fn -> Dynamo.become_virtual_node(base_config_a) end)
    spawn(:b, fn -> Dynamo.become_virtual_node(base_config_b) end)
    spawn(:c, fn -> Dynamo.become_virtual_node(base_config_c) end)

    put_client =
      spawn(:put_client, fn ->
        put_client = Dynamo.Client.new_client([:a, :b, :c])
        # first put an old version data
        {:ok , put_client} = Dynamo.Client.put(put_client, 5, "x")
        {:ok , put_client} = Dynamo.Client.put(put_client, 5, "y")
      end)

    get_client =
      spawn(:get_client, fn ->
        mark_unfuzzable()
        get_client = Dynamo.Client.new_client([:a, :b, :c])
        Process.sleep(20)
        # {{:value, v}, get_client} = Dynamo.Client.get_and_listen(get_client, 5)
        results = Dynamo.Client.periodical_get(get_client, get_interval, 0, num_trials, 5, [])
        IO.puts("got results: #{inspect(results)}")
        assert length(results) == num_trials
      end)

    handle = Process.monitor(get_client)
  # Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      30_000 -> assert false
    end
  after
    Emulation.terminate()
  end
end
