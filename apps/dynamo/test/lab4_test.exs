defmodule Lab4Test do
  use ExUnit.Case
  doctest Raft
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Leader election eventually produces a leader" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    base_config =
      Raft.new_configuration([:a, :b, :c], :a, 100_000, 100_001, 1000)

    spawn(:b, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:c, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:a, fn -> Raft.become_leader(base_config) end)
    caller = self()
    client =
      spawn(:client, fn ->
        send(:c, {:set_election_timeout, 250, 251})

        # Change the election timeout for :c, this should
        # ensure that :c becomes the next leader.
        receive do
          {:c, :ok} -> :ok
          _ -> send(caller, :fail)
        end

        # Give things a bit of time to settle down.
        receive do
        after
          1_000 -> :ok
        end

        view = [:a, :b, :c]
        view |> Enum.map(fn x -> send(x, :whois_leader) end)

        leaders =
          view
          |> Enum.map(fn x ->
            receive do
              {^x, leader} -> leader
            end
          end)
          |> Enum.uniq()
        IO.inspect(leaders)
        assert length(leaders) == 1, "More than one unique leader found."

        view |> Enum.map(fn x -> send(x, :current_process_type) end)

        types =
          view
          |> Enum.map(fn x ->
            receive do
              {^x, leader} -> leader
            end
          end)

        l_count = Enum.count(types, fn t -> t == :leader end)
        f_count = Enum.count(types, fn t -> t == :follower end)

        assert l_count == 1
        assert f_count + l_count == length(view)
      end)

    # Timeout.
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

  test "Operations work in the midst of leader election" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(10)])

    base_config =
      Raft.new_configuration([:a, :b, :c], :a, 100_000, 100_001, 1000)

    spawn(:b, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:c, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:a, fn -> Raft.become_leader(base_config) end)

    client =
      spawn(:client, fn ->
        send(:c, {:set_election_timeout, 250, 251})

        # Change the election timeout for :c, this should
        # ensure that :c becomes the next leader.
        receive do
          {:c, :ok} -> :ok
          _ -> assert false
        end

        # Wait for election
        receive do
        after
          190 -> :ok
        end

        client = Raft.Client.new_client(:b)
        {:ok, client} = Raft.Client.enq(client, 5)
        {{:value, v}, client} = Raft.Client.deq(client)
        assert v == 5
        {v, _} = Raft.Client.deq(client)
        assert v == :empty
      end)

    # Timeout.
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

  test "Check leader stability" do
    IO.puts("-------Here WE START LEADER STABILITY TEST----")
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(10)])

    base_config = Raft.new_configuration([:a, :b, :c], :a, 498, 501, 100)

    spawn(:b, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:c, fn -> Raft.become_follower(Raft.make_follower(base_config)) end)
    spawn(:a, fn -> Raft.become_leader(base_config) end)

    client =
      spawn(:client, fn ->
        view = [:a, :b, :c]
        # Wait a tiny bit initially.
        receive do
        after
          20 -> :ok
        end

        view |> Enum.map(fn x -> send(x, :whois_leader) end)

        original_leader =
          view
          |> Enum.map(fn x ->
            receive do
              {^x, leader} -> leader
            end
          end)
          |> Enum.uniq()
        IO.inspect(original_leader)
        assert length(original_leader) == 1, "More than one leader found."

        # Wait for a couple of elections
        receive do
        after
          1_000 -> :ok
        end

        view |> Enum.map(fn x -> send(x, :whois_leader) end)
        later_leader =
          view
          |> Enum.map(fn x ->
            receive do
              {^x, leader} -> leader
            end
          end)
          |> Enum.uniq()
        IO.inspect(original_leader)
        assert length(later_leader) == 1, "More than one leader found."
        assert later_leader == original_leader, "Leadership changed."
      end)

    # Timeout.
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
