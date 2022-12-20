defmodule HashTest do
  use ExUnit.Case
  doctest Dynamo
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "we can find coordinate node and return value" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    w = r = n = 1
    # base_config_a = Dynamo.new_configuration([:b, :c], %{a: [240,360], b: [0,120], c: [120, 240]}, 10, :client, w, r, n)
    base_config_a = Dynamo.new_configuration([:b, :c], %{a: [0,120], b: [120,240], c: [240, 360]}, 10, :client, w, r, n)
    base_config_b = Dynamo.new_configuration([:c, :a], %{a: [0,120], b: [120,240], c: [240, 360]}, 10, :client, w, r, n)
    base_config_c = Dynamo.new_configuration([:a, :b], %{a: [0,120], b: [120,240], c: [240, 360]}, 10, :client, w, r, n)

    base_config_a = Dynamo.new_configuration([:b, :c], %{a: [0,120], b: [120,240], c: [240, 360]}, 10, :client, w, r, n)


    spawn(:a, fn -> Dynamo.become_virtual_node(base_config_a) end)
    spawn(:b, fn -> Dynamo.become_virtual_node(base_config_b) end)
    spawn(:c, fn -> Dynamo.become_virtual_node(base_config_c) end)

    client =
      spawn(:client, fn ->
        client = Dynamo.Client.new_client([:a, :b, :c])
        {:ok, client} = Dynamo.Client.put(client, 5, "x")
        IO.puts("successfully put.")
        {{:value, v}, client} = Dynamo.Client.get(client, 5)
        assert v == [{"x", %{a: 1}}]
      end)

    handle = Process.monitor(client)
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
