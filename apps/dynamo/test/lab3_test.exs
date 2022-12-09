defmodule Lab3Test do
  use ExUnit.Case
  doctest Raft
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Nothing crashes during startup and heartbeats" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    base_config =
      Raft.new_configuration([:a, :b, :c], :a, 100_000, 100_001, 1000)

    spawn(:b, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:c, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:a, fn -> Raft.become_leader(base_config) end)

    client =
      spawn(:client, fn ->
        client = Raft.Client.new_client(:c)
        Raft.Client.nop(client)

        receive do
        after
          5_000 -> true
        end
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

  test "RSM operations work" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    base_config =
      Raft.new_configuration([:a, :b, :c], :a, 100_000, 100_001, 1000)

    spawn(:b, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:c, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:a, fn -> Raft.become_leader(base_config) end)

    client =
      spawn(:client, fn ->
        client = Raft.Client.new_client(:c)
        {:ok, client} = Raft.Client.enq(client, 5)
        {{:value, v}, client} = Raft.Client.deq(client)
        assert v == 5
        {v, _} = Raft.Client.deq(client)
        assert v == :empty
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

  test "RSM Logs are correctly updated" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    base_config =
      Raft.new_configuration([:a, :b, :c], :a, 100_000, 100_001, 1000)

    spawn(:b, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:c, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:a, fn -> Raft.become_leader(base_config) end)

    client =
      spawn(:client, fn ->
        view = [:a, :b, :c]
        client = Raft.Client.new_client(:c)
        # Perform one operation
        {:ok, _} = Raft.Client.enq(client, 5)
        # Now collect logs
        view |> Enum.map(fn x -> send(x, :send_log) end)

        logs =
          view
          |> Enum.map(fn x ->
            receive do
              {^x, log} -> log
            end
          end)

        log_lengths = logs |> Enum.map(&length/1)

        assert Enum.count(log_lengths, fn l -> l == 1 end) >= 2 &&
                 !Enum.any?(log_lengths, fn l -> l > 1 end)
      end)

    handle = Process.monitor(client)
    # Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      30_000 -> false
    end
  after
    Emulation.terminate()
  end

  test "RSM replicas commit correctly" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    base_config =
      Raft.new_configuration([:a, :b, :c], :a, 100_000, 100_001, 1000)

    spawn(:b, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:c, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:a, fn -> Raft.become_leader(base_config) end)

    client =
      spawn(:client, fn ->
        view = [:a, :b, :c]
        client = Raft.Client.new_client(:c)
        # Perform one operation
        {:ok, client} = Raft.Client.enq(client, 5)
        # Now use a nop to force a commit.
        {:ok, _} = Raft.Client.nop(client)
        # Now collect queues
        view |> Enum.map(fn x -> send(x, :send_state) end)

        queues =
          view
          |> Enum.map(fn x ->
            receive do
              {^x, s} -> s
            end
          end)

        q_lengths = queues |> Enum.map(&:queue.len/1)

        assert Enum.count(q_lengths, fn l -> l == 1 end) >= 2 &&
                 !Enum.any?(q_lengths, fn l -> l > 1 end)

        q_values =
          queues
          |> Enum.map(&:queue.out/1)
          |> Enum.map(fn {v, _} -> v end)

        assert Enum.all?(q_values, fn l -> l == {:value, 5} || l == :empty end)
      end)

    handle = Process.monitor(client)
    # Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      30_000 -> false
    end
  after
    Emulation.terminate()
  end
end
