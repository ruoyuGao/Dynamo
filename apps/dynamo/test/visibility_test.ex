defmodule DataversionTest do
    use ExUnit.Case
    doctest Dynamo
    import Emulation, only: [spawn: 2, send: 2, mark_unfuzzable: 0]

    import Kernel,
      except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
    test "can return the data after time, pbs t-visibility" do
      Emulation.init()
      #Emulation.append_fuzzers([Fuzzers.delay(2),Fuzzers.drop(0.5)])
      Emulation.append_fuzzers([Fuzzers.delay(10.0)])
      w = r = n = 1
      # base_config_a = Dynamo.new_configuration([:b, :c], %{a: [240,360], b: [0,120], c: [120, 240]}, 1000, :client, w, r, n)
      base_config_a = Dynamo.new_configuration([:b, :c], %{a: [0,120], b: [120,240], c: [240, 360]}, 1000, w, r, n)
      base_config_b = Dynamo.new_configuration([:c, :a], %{a: [0,120], b: [120,240], c: [240, 360]}, 1000, w, r, n)
      base_config_c = Dynamo.new_configuration([:a, :b], %{a: [0,120], b: [120,240], c: [240, 360]}, 1000, w, r, n)

      # base_config_a = Dynamo.new_configuration([:b, :c], %{a: [0,120], b: [120,240], c: [240, 360]}, 10, :client, w, r, n)


      spawn(:a, fn -> Dynamo.become_virtual_node(base_config_a) end)
      spawn(:b, fn -> Dynamo.become_virtual_node(base_config_b) end)
      spawn(:c, fn -> Dynamo.become_virtual_node(base_config_c) end)

      # client =
      #   spawn(:client, fn ->
      #     client = Dynamo.Client.new_client([:a, :b, :c])
      #     {{:value, v}, client} = Dynamo.Client.put_and_get(client, 5, "x")
      #     assert v == [{"x", %{a: 1}}]
      #     # assert length(v) > 1
      #   end)
      put_client =
        spawn(:put_client, fn ->
          put_client = Dynamo.Client.new_client([:a, :b, :c])
          {:ok , put_client} = Dynamo.Client.put(put_client, 5, "x")
        end)

      get_client =
        spawn(:get_client, fn ->
          mark_unfuzzable()
          get_client = Dynamo.Client.new_client([:a, :b, :c])
          {{:value, v}, get_client} = Dynamo.Client.get_and_listen(get_client, 5)
          IO.puts("got v: #{inspect(v)}")
          assert assert v == [{"x", %{a: 1}}]
        end)

      handle = Process.monitor(get_client)
    # Timeout.
      receive do
        {:DOWN, ^handle, _, _, _} ->
          IO.puts(">>>>pass the t-visibility test")
          true
      after
        30_000 -> assert false
      end
    after
      Emulation.terminate()
    end
  end
