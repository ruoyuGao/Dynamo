defmodule LogTest do
  use ExUnit.Case
  doctest Raft

  defp create_empty_log() do
    Raft.new_configuration([:a, :b, :c], :a, 100, 1000, 20)
  end

  defp create_nop_log(last_index) do
    config = create_empty_log()
    log = for idx <- last_index..1, do: Raft.LogEntry.nop(idx, 1, :a)
    %{config | log: log}
  end

  test "Last index and term work" do
    assert Raft.get_last_log_index(create_empty_log()) == 0
    assert Raft.get_last_log_index(create_nop_log(10)) == 10
    assert Raft.get_last_log_term(create_empty_log()) == 0
    assert Raft.get_last_log_term(create_nop_log(10)) == 1
  end

  test "logged? works" do
    assert Raft.logged?(create_empty_log(), 1) == false
    assert Raft.logged?(create_nop_log(10), 1) == true
  end

  test "get_log_entry works" do
    assert Raft.get_log_entry(create_empty_log(), 2) == :noentry
    entry = Raft.get_log_entry(create_nop_log(10), 5)
    assert entry.index == 5
    assert entry.term == 1
    assert Raft.get_log_entry(create_nop_log(10), 0) == :noentry
  end

  test "truncate_log_at_index works" do
    config = create_nop_log(20)
    trunc_9 = Raft.truncate_log_at_index(config, 10)
    assert Raft.get_last_log_index(trunc_9) == 9
    assert Raft.get_last_log_term(trunc_9) == 1
    trunc_19 = Raft.truncate_log_at_index(config, 20)
    assert Raft.get_last_log_index(trunc_19) == 19
    assert Raft.get_last_log_term(trunc_19) == 1
    trunc_20 = Raft.truncate_log_at_index(config, 21)
    assert Raft.get_last_log_index(trunc_20) == 20
    assert Raft.get_last_log_term(trunc_20) == 1
  end

  test "get_log_suffix works" do
    config = create_nop_log(20)
    assert length(Raft.get_log_suffix(config, 10)) == 11
    assert length(Raft.get_log_suffix(config, 20)) == 1
    assert Raft.get_log_suffix(config, 21) == []
  end

  test "add_log_entries works" do
    config = create_nop_log(2)
    entries = [Raft.LogEntry.nop(3, 2, :b)]
    config = Raft.add_log_entries(config, entries)
    assert Raft.get_last_log_index(config) == 3
    assert Raft.get_last_log_term(config) == 2
    config = Raft.add_log_entries(config, [])
    assert Raft.get_last_log_index(config) == 3
    assert Raft.get_last_log_term(config) == 2
  end
end
