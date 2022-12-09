defmodule EmulationTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2, send: 2, broadcast: 1, timer: 1]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  import Fuzzers, only: [delay: 1, log_messages: 0]

  def ping do
    receive do
      {sender, :ping} -> send(sender, :pong)
      m -> assert(false, "Expected message with :ping, got #{m}")
    end
  end

  def pong(caller, ping) do
    send(ping, :ping)

    receive do
      {^ping, :pong} -> send(caller, :done)
      m -> assert(false, "Expected #{inspect({ping, :pong})} got #{inspect(m)}")
    end
  end

  def timer_test do
    timer(100)

    receive do
      m -> assert(m == :timer, "Expected #{inspect(:timer)} got #{inspect(m)}")
    end
  end

  test "Message sends work" do
    Emulation.init()
    pid = self()
    spawn(:ping, &ping/0)
    spawn(:pong, fn -> pong(pid, :ping) end)

    receive do
      :done -> assert true
      _ -> assert false
    end
  end

  test "Logging and delays works" do
    Emulation.init()
    pid = self()
    logging = log_messages()
    Emulation.append_fuzzers([delay(1.0), logging])
    spawn(:ping, &ping/0)
    spawn(:pong, fn -> pong(pid, :ping) end)

    receive do
      m -> assert(m == :done)
    end

    logs = Fuzzers.get_logs(logging)
    assert(length(logs) == 2)
  end

  test "Broadcast works" do
    Emulation.init()
    spawn(:ping1, &ping/0)
    spawn(:ping2, &ping/0)
    broadcast(:ping)

    receive do
      m -> assert(m == :pong)
    end

    receive do
      m -> assert(m == :pong)
    end
  end

  test "Timer works" do
    Emulation.init()
    p = spawn(:timer, &timer_test/0)
    r = Process.monitor(p)
    Process.send_after(self(), :timeout, 1000)

    receive do
      {:DOWN, ^r, _, _, _} ->
        assert(true)

      :timeout ->
        assert(false, "Timer did not fire?")
    end

    Emulation.terminate()
  end

  defp test_time do
    t0 = Emulation.now()
    t1 = Emulation.now()
    assert t1 >= t0, "Go forward"
    new_base = 10_000_000_000
    Emulation.set_time(new_base)
    t2 = Emulation.now()
    t3 = Emulation.now()
    assert t2 >= new_base
    assert t3 >= new_base
    assert t3 >= t2
  end

  test "Time works" do
    Emulation.init()
    p = spawn(:time_test, &test_time/0)
    r = Process.monitor(p)

    receive do
      {:DOWN, ^r, _, _, _} ->
        assert(true)
    end

    Emulation.terminate()
  end
end
