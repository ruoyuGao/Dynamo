defmodule Raft do
  @moduledoc """
  An implementation of the Raft consensus protocol.
  """
  # Shouldn't need to spawn anything from this module, but if you do
  # you should add spawn to the imports.
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  # This allows you to use Elixir's loggers
  # for messages. See
  # https://timber.io/blog/the-ultimate-guide-to-logging-in-elixir/
  # if you are interested in this. Note we currently purge all logs
  # below Info
  require Logger

  # This structure contains all the process state
  # required by the Raft protocol.
  defstruct(
    # The list of current proceses.
    view: nil,
    # Current leader.
    current_leader: nil,
    # Time before starting an election.
    min_election_timeout: nil,
    max_election_timeout: nil,
    election_timer: nil,
    # Time between heartbeats from the leader.
    heartbeat_timeout: nil,
    heartbeat_timer: nil,
    # Persistent state on all servers.
    current_term: nil,
    voted_for: nil,
    # A short note on log structure: The functions that follow
    # (e.g., get_last_log_index, commit_log_index, etc.) all
    # assume that the log is a list with later entries (i.e.,
    # entries with higher index numbers) appearing closer to
    # the head of the list, and that index numbers start with 1.
    # For example if the log contains 3 entries committe in term
    # 2, 2, and 1 we would expect:
    #
    # `[{index: 3, term: 2, ..}, {index: 2, term: 2, ..},
    #     {index: 1, term: 1}]`
    #
    # If you change this structure, you will need to change
    # those functions.
    #
    # Finally, it might help to know that two lists can be
    # concatenated using `l1 ++ l2`
    log: nil,
    # Volatile state on all servers
    commit_index: nil,
    last_applied: nil,
    # Volatile state on leader
    is_leader: nil,
    next_index: nil,
    match_index: nil,
    # The queue we are building using this RSM.
    queue: nil
  )

  @doc """
  Create state for an initial Raft cluster. Each
  process should get an appropriately updated version
  of this state.
  """
  @spec new_configuration(
          [atom()],
          atom(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: %Raft{}
  def new_configuration(
        view,
        leader,
        min_election_timeout,
        max_election_timeout,
        heartbeat_timeout
      ) do
    %Raft{
      view: view,
      current_leader: leader,
      min_election_timeout: min_election_timeout,
      max_election_timeout: max_election_timeout,
      heartbeat_timeout: heartbeat_timeout,
      # Start from term 1
      current_term: 1,
      voted_for: nil,
      log: [],
      commit_index: 0,
      last_applied: 0,
      is_leader: false,
      next_index: nil,
      match_index: nil,
      queue: :queue.new()
    }
  end

  # Enqueue an item, this **modifies** the state
  # machine, and should only be called when a log
  # entry is committed.
  @spec enqueue(%Raft{}, any()) :: %Raft{}
  defp enqueue(state, item) do
    %{state | queue: :queue.in(item, state.queue)}
  end

  # Dequeue an item, modifying the state machine.
  # This function should only be called once a
  # log entry has been committed.
  @spec dequeue(%Raft{}) :: {:empty | {:value, any()}, %Raft{}}
  defp dequeue(state) do
    {ret, queue} = :queue.out(state.queue)
    {ret, %{state | queue: queue}}
  end

  @doc """
  Commit a log entry, advancing the state machine. This
  function returns a tuple:
  * The first element is {requester, return value}. Your
    implementation should ensure that the leader who committed
    the log entry sends the return value to the requester.
  * The second element is the updated state.
  """
  @spec commit_log_entry(%Raft{}, %Raft.LogEntry{}) ::
          {{atom() | pid(), :ok | :empty | {:value, any()}}, %Raft{}}
  def commit_log_entry(state, entry) do
    case entry do
      %Raft.LogEntry{operation: :nop, requester: r, index: i} ->
        {{r, :ok}, %{state | commit_index: i}}

      %Raft.LogEntry{operation: :enq, requester: r, argument: e, index: i} ->
        {{r, :ok}, %{enqueue(state, e) | commit_index: i}}

      %Raft.LogEntry{operation: :deq, requester: r, index: i} ->
        {ret, state} = dequeue(state)
        {{r, ret}, %{state | commit_index: i}}

      %Raft.LogEntry{} ->
        raise "Log entry with an unknown operation: maybe an empty entry?"

      _ ->
        raise "Attempted to commit something that is not a log entry."
    end
  end
  @doc """
  When last_applied index < commit index we want, we apply all the entry between last applied and commited index(included)
  """
  @spec apply_log_index(%Raft{},non_neg_integer()):: %Raft{}
  def apply_log_index(state,commit_index_need) do
    IO.puts("start to apply")
    if state.last_applied < commit_index_need do
      IO.puts("start add last applied and update queue")
      last_applied_index  = state.last_applied
      IO.puts("log len is #{length(state.log)}")
      {{requester,ok_item},state} = commit_log_index(state,last_applied_index+1)
      state = %{state | last_applied: last_applied_index+1}
      if state.is_leader == true do
        IO.puts("leader send response to client")
        send(requester, ok_item)
      end
      apply_log_index(state,commit_index_need)
    else
      IO.puts("finish update")
      state
    end
  end
  @doc """
  Commit log at index `index`. This index, which one should read from
  the log entry is assumed to start at 1. This function **does not**
  ensure that commits are processed in order.
  """
  @spec commit_log_index(%Raft{}, non_neg_integer()) ::
          {:noentry | {atom(), :ok | :empty | {:value, any()}}, %Raft{}}
  def commit_log_index(state, index) do
    if length(state.log) < index do
      {:noentry, state}
    else
      # Note that entry indexes are all 1, which in
      # turn means that we expect commit indexes to
      # be 1 indexed. Now a list is a reversed log,
      # so what we can do here is simple:
      # Given 0-indexed index i, length(log) - 1 - i
      # is the ith list element. => length(log) - (i +1),
      # and hence length(log) - index is what we want.
      correct_idx = length(state.log) - index
      commit_log_entry(state, Enum.at(state.log, correct_idx))
    end
  end
  # The next few functions are public so we can test them, see
  # log_test.exs.
  @doc """
  Get index for the last log entry.
  """
  @spec get_last_log_index(%Raft{}) :: non_neg_integer()
  def get_last_log_index(state) do
    Enum.at(state.log, 0, Raft.LogEntry.empty()).index
  end

  @spec get_last_entries_index([%Raft.LogEntry{}]) :: non_neg_integer()
  def get_last_entries_index(entries) do
    Enum.at(entries, 0, Raft.LogEntry.empty()).index
  end
  @doc """
  Get term for the last log entry.
  """
  @spec get_last_log_term(%Raft{}) :: non_neg_integer()
  def get_last_log_term(state) do
    Enum.at(state.log, 0, Raft.LogEntry.empty()).term
  end

  @doc """
  Check if log entry at index exists.
  """
  @spec logged?(%Raft{}, non_neg_integer()) :: boolean()
  def logged?(state, index) do
    index > 0 && length(state.log) >= index
  end

  @doc """
  Get log entry at `index`.
  """
  @spec get_log_entry(%Raft{}, non_neg_integer()) ::
          :no_entry | %Raft.LogEntry{}
  def get_log_entry(state, index) do
    if index <= 0 || length(state.log) < index do
      :noentry
    else
      # Note that entry indexes are all 1, which in
      # turn means that we expect commit indexes to
      # be 1 indexed. Now a list is a reversed log,
      # so what we can do here is simple:
      # Given 0-indexed index i, length(log) - 1 - i
      # is the ith list element. => length(log) - (i +1),
      # and hence length(log) - index is what we want.
      correct_idx = length(state.log) - index
      Enum.at(state.log, correct_idx)
    end
  end

  @spec get_entries_entry([%Raft.LogEntry{}],non_neg_integer()) :: :no_entry | %Raft.LogEntry{}
  def get_entries_entry(entries,index) do
    if index <= 0 || length(entries) < index do
      :noentry
    else
      correct_index = length(entries) - index
      Enum.at(entries,correct_index)
    end
  end
  @doc """
  Get log entries starting at index.
  """
  @spec get_log_suffix(%Raft{}, non_neg_integer()) :: [%Raft.LogEntry{}]
  def get_log_suffix(state, index) do
    if length(state.log) < index do
      []
    else
      correct_idx = length(state.log) - index
      Enum.take(state.log, correct_idx + 1)
    end
  end

  @doc """
  Truncate log entry at `index`. This removes log entry
  with index `index` and larger.
  """
  @spec truncate_log_at_index(%Raft{}, non_neg_integer()) :: %Raft{}
  def truncate_log_at_index(state, index) do
    if length(state.log) < index do
      # Nothing to do
      state
    else
      to_drop = length(state.log) - index + 1
      %{state | log: Enum.drop(state.log, to_drop)}
    end
  end
  @doc """
  Truncate entries entry at index, this removes entries entry will keep with index 'index' but remove larger
  """
  @spec truncate_entries_at_index([%Raft.LogEntry{}],non_neg_integer())::[%Raft.LogEntry{}]
  def truncate_entries_at_index(entries,index) do
    if length(entries) < index do
      entries
    else
      IO.puts("entries len is #{length(entries)}")
      to_drop = length(entries) - index
      Enum.drop(entries,to_drop) # not sure it will return a entry list, take care
    end
  end
  @doc """
  Add log entries to the log. This adds entries to the beginning
  of the log, we assume that entries are already correctly ordered
  (see structural note about log above.).
  """
  @spec add_log_entries(%Raft{}, [%Raft.LogEntry{}]) :: %Raft{}
  def add_log_entries(state, entries) do
    %{state | log: entries ++ state.log}
  end

  @doc """
  make_leader changes process state for a process that
  has just been elected leader.
  """
  @spec make_leader(%Raft{}) :: %Raft{
          is_leader: true,
          next_index: map(),
          match_index: map()
        }
  def make_leader(state) do
    log_index = get_last_log_index(state)

    # next_index needs to be reinitialized after each
    # election.
    next_index =
      state.view
      |> Enum.map(fn v -> {v, log_index} end)
      |> Map.new()

    # match_index needs to be reinitialized after each
    # election.
    match_index =
      state.view
      |> Enum.map(fn v -> {v, 0} end)
      |> Map.new()

    %{
      state
      | is_leader: true,
        next_index: next_index,
        match_index: match_index,
        current_leader: whoami()
    }
  end
  @doc """
  make_follower changes process state for a process
  to mark it as a follower.
  """
  @spec make_follower(%Raft{}) :: %Raft{
          is_leader: false
        }
  def make_follower(state) do
    %{state | is_leader: false}
  end

  # update_leader: update the process state with the
  # current leader.
  @spec update_leader(%Raft{}, atom()) :: %Raft{current_leader: atom()}
  defp update_leader(state, who) do
    %{state | current_leader: who}
  end

  # Compute a random election timeout between
  # state.min_election_timeout and state.max_election_timeout.
  # See the paper to understand the reasoning behind having
  # a randomized election timeout.
  @spec get_election_time(%Raft{}) :: non_neg_integer()
  defp get_election_time(state) do
    state.min_election_timeout +
      :rand.uniform(
        state.max_election_timeout -
          state.min_election_timeout
      )
  end

  # Save a handle to the election timer.
  @spec save_election_timer(%Raft{}, reference()) :: %Raft{}
  defp save_election_timer(state, timer) do
    %{state | election_timer: timer}
  end

  # Save a handle to the hearbeat timer.
  @spec save_heartbeat_timer(%Raft{}, reference()) :: %Raft{}
  defp save_heartbeat_timer(state, timer) do
    %{state | heartbeat_timer: timer}
  end

  # Utility function to send a message to all
  # processes other than the caller. Should only be used by leader.
  @spec broadcast_to_others(%Raft{is_leader: true}, any()) :: [boolean()]
  defp broadcast_to_others(state, message) do
    me = whoami()

    state.view
    |> Enum.filter(fn pid -> pid != me end)
    |> Enum.map(fn pid -> send(pid, message) end)
  end

  # END OF UTILITY FUNCTIONS. You should not need to (but are allowed to)
  # change any of the code above this line, but will definitely need to
  # change the code that follows.

  # This function should cancel the current
  # election timer, and set  a new one. You can use
  # `get_election_time` defined above to get a
  # randomized election timeout. You might need
  # to call this function from within your code.
  @spec reset_election_timer(%Raft{}) :: %Raft{}
  defp reset_election_timer(state) do
    # TODO: Set a new election timer
    # You might find `save_election_timer` of use.
    new_election_time = get_election_time(state)
    if state.election_timer != nil do
      Emulation.cancel_timer(state.election_timer)
    end
    new_election_timer = Emulation.timer(new_election_time)
    save_election_timer(state,new_election_timer)
  end

  # This function should cancel the current
  # hearbeat timer, and set  a new one. You can
  # get heartbeat timeout from `state.heartbeat_timeout`.
  # You might need to call this from your code.
  @spec reset_heartbeat_timer(%Raft{}) :: %Raft{}
  defp reset_heartbeat_timer(state) do
    # TODO: Set a new heartbeat timer.
    # You might find `save_heartbeat_timer` of use.
    if state.heartbeat_timer != nil do
      Emulation.cancel_timer(state.heartbeat_timer)
    end
    new_heartbeat_timer = Emulation.timer(state.heartbeat_timeout)
    save_heartbeat_timer(state,new_heartbeat_timer)
  end

  @doc """
  This function transitions a process so it is
  a follower.
  """
  @spec become_follower(%Raft{}) :: no_return()
  def become_follower(state) do
    # TODO: Do anything you need to when a process
    # transitions to a follower.
    # remove raise problem and not sure it can work
    state = reset_election_timer(state)
    follower(make_follower(state), nil)
  end

  @doc """
  this function used for compare term at same index, if same go to next, else return the index
  """
  @spec compare_log_entries(%Raft{},[%Raft.LogEntry{}],non_neg_integer())::non_neg_integer()
  def compare_log_entries(state,entries,current_index) do
    if get_log_entry(state,current_index) != :noentry && get_log_entry(state,current_index).term == get_entries_entry(entries,current_index) do
      compare_log_entries(state,entries,current_index+1)
    else
      current_index
    end
  end
  @doc """
  This function implements the state machine for a process
  that is currently a follower.

  `extra_state` can be used to hod anything that you find convenient
  when building your implementation.
  """
  @spec follower(%Raft{is_leader: false}, any()) :: no_return()
  def follower(state, extra_state) do
    receive do
      # messages from timer that do not receive heartbeat
      :timer ->
        IO.puts("Election timeout, follower start to transfer to candidate")
        # reset election timer
        state = reset_election_timer(state)
        # increment currentTerm
        new_current_term = state.current_term + 1
        state = %{state | current_term: new_current_term}
        # vote for self
        state = %{state | voted_for: whoami()}
        #broad cast others for requestVote
        last_log_index = get_last_log_index(state)
        last_log_term = get_last_log_term(state)
        send_RequestVote = Raft.RequestVote.new(state.current_term,whoami(),last_log_index,last_log_term)
        broadcast_to_others(state,send_RequestVote)
        IO.puts("broadcast requestVote to others,inspect requestVote message here")
        IO.inspect(send_RequestVote)
        become_candidate(state)
      # Messages that are a part of Raft.
      {sender,
       %Raft.AppendEntryRequest{
         term: term,
         leader_id: leader_id,
         prev_log_index: prev_log_index,
         prev_log_term: prev_log_term,
         entries: entries,
         leader_commit_index: leader_commit_index
       }} ->
        # TODO: Handle an AppendEntryRequest received by a
        # follower
        if state.current_term > term do
          IO.puts("current term bigger than request term, return notok")
          send(sender,:notok)
        end
        if entries == nil do
          # received heartbeat, change leader id or make sure follower follow the right leader
          state = update_leader(state,leader_id)
          state = %{state| current_term: term}
          state = %{state|voted_for: nil}
          state = reset_election_timer(state)
          IO.puts("#{whoami()}finish update ledaer id with heart beat #{state.current_leader}")
          #IO.inspect(state.current_leader)
          follower(state,extra_state)
          IO.puts("recursing new follower, folloing part will not execute")
        end
        if  get_log_entry(state,prev_log_index) != :noentry && get_log_entry(state,prev_log_index).term != prev_log_term do
          IO.puts("log[prev_log_index].term != prev_log_term return :notok")
          send(sender,:notok)
        else
          IO.puts("compare the index and term from prev_log_index find the first difference index")
          first_diff_index = compare_log_entries(state,entries,prev_log_index+1)
          IO.puts("first diff index is#{first_diff_index}")
          state = add_log_entries(truncate_log_at_index(state,first_diff_index),truncate_entries_at_index(entries,first_diff_index))
          IO.puts("log len is #{length(state.log)}")
          if leader_commit_index > state.commit_index do
            commit_index_need_follower = min(leader_commit_index,get_last_log_index(state))
            IO.puts("commit_index_follower is #{commit_index_need_follower}")
            state = apply_log_index(state,commit_index_need_follower)
            #state = %{state | commit_index: commit_index_need_follower}
            send_appendResponse = Raft.AppendEntryResponse.new(
              state.current_term,
              get_last_log_index(state),
              true
            )
            IO.puts("start send response to leader")
            send(sender,send_appendResponse)
            follower(state,extra_state)
          end

          #state = %{state | commit_index: commit_index_need_follower}
          send_appendResponse = Raft.AppendEntryResponse.new(
            state.current_term,
            get_last_log_index(state),
            true
          )
          IO.puts("start send response to leader")
          send(sender,send_appendResponse)
          follower(state,extra_state)
        end
      {sender,
       %Raft.AppendEntryResponse{
         term: term,
         log_index: index,
         success: succ
       }} ->
        # TODO: Handle an AppendEntryResponse received by
        # a follower.
        IO.puts("received message should to leader not follower, but we can check term for it")
        if state.current_term < term do
          state = %{state| current_term: term}
          state = %{state|voted_for: nil}
          follower(state,extra_state)
        end
        send(sender,:notleader)
        IO.puts(
          "Follower received append entry response #{term}," <>
            " index #{index}, succcess #{inspect(succ)}"
        )

      {sender,
       %Raft.RequestVote{
         term: term,
         candidate_id: candidate,
         last_log_index: last_log_index,
         last_log_term: last_log_term
       }} ->
        # TODO: Handle a RequestVote call received by a
        # follower.
        if state.current_term > term do
          send_RequestVoteResponse = Raft.RequestVoteResponse.new(state.current_term,false)
          send(sender,send_RequestVoteResponse)
        end
        if last_log_index >= get_last_log_index(state) && last_log_term>= get_last_log_term(state) do
          if state.voted_for == nil do
            state = %{state| voted_for: candidate}
            send_RequestVoteResponse = Raft.RequestVoteResponse.new(state.current_term,true)
            IO.puts("follower received requestVote from candidadte, send response to candidate")
            IO.inspect(send_RequestVoteResponse)
            send(sender,send_RequestVoteResponse)
            follower(state,extra_state)
          end
        end
        follower(state,extra_state)
        IO.puts(
          "Follower received RequestVote " <>
            "term = #{term}, candidate = #{candidate}"
        )

      {sender,
       %Raft.RequestVoteResponse{
         term: term,
         granted: granted
       }} ->
        # TODO: Handle a RequestVoteResponse.
        IO.puts("follower received reqiestvode response, which should been received by candidate")
        follower(state,extra_state)
        IO.puts(
          "Follower received RequestVoteResponse " <>
            "term = #{term}, granted = #{inspect(granted)}"
        )
      # Messages from external clients. In each case we
      # tell the client that it should go talk to the
      # leader.
      {sender, :nop} ->
        send(sender, {:redirect, state.current_leader})
        follower(state, extra_state)

      {sender, {:enq, item}} ->
        send(sender, {:redirect, state.current_leader})
        follower(state, extra_state)

      {sender, :deq} ->
        send(sender, {:redirect, state.current_leader})
        follower(state, extra_state)

      # Messages for debugging [Do not modify existing ones,
      # but feel free to add new ones.]
      {sender, :send_state} ->
        send(sender, state.queue)
        follower(state, extra_state)

      {sender, :send_log} ->
        send(sender, state.log)
        follower(state, extra_state)

      {sender, :whois_leader} ->
        send(sender, {state.current_leader, state.current_term})
        follower(state, extra_state)

      {sender, :current_process_type} ->
        send(sender, :follower)
        follower(state, extra_state)

      {sender, {:set_election_timeout, min, max}} ->
        state = %{state | min_election_timeout: min, max_election_timeout: max}
        state = reset_election_timer(state)
        IO.puts("#{whoami()} received signal, perpare to be timeout")
        send(sender, :ok)
        follower(state, extra_state)

      {sender, {:set_heartbeat_timeout, timeout}} ->
        send(sender, :ok)
        follower(%{state | heartbeat_timeout: timeout}, extra_state)
    end
  end

  @doc """
  This function transitions a process that is not currently
  the leader so it is a leader.
  """
  @spec become_leader(%Raft{is_leader: false}) :: no_return()
  def become_leader(state) do
    # TODO: Send out any one time messages that need to be sent,
    # you might need to update the call to leader too.
    # remove raise mot implement problem, but not sure we need to add more
    # we create a extra state to store how many follower has send responese correct to leader at specific index
    # n is the maxmum index for log
    n = 10
    extra_state = List.duplicate(0,n)
    IO.puts("reset heartbeat timer")
    state = reset_heartbeat_timer(state)
    IO.puts("candidate update itself to leader #{whoami()}")
    state = update_leader(state,whoami())
    leader(make_leader(state), extra_state)
  end

  @doc """
  This function implements the state machine for a process
  that is currently the leader.

  `extra_state` can be used to hold any additional information.
  HINT: It might be useful to track the number of responses
  received for each AppendEntry request.
  """
  @spec leader(%Raft{is_leader: true}, any()) :: no_return()
  def leader(state, extra_state) do
    receive do
      :timer ->
        IO.puts("heartbeat time is ok, start send blank appendEntryRequest to others")
        send_AppendEntryRequest = Raft.AppendEntryRequest.new(
          state.current_term,
          state.current_leader,
          1,
          1,
          nil,
          1
        )
        broadcast_to_others(state,send_AppendEntryRequest)
        IO.puts("reset heartbeat timer")
        state = reset_heartbeat_timer(state)
        leader(state,extra_state)
      # Messages that are a part of Raft.
      {sender,
       %Raft.AppendEntryRequest{
         term: term,
         leader_id: leader_id,
         prev_log_index: prev_log_index,
         prev_log_term: prev_log_term,
         entries: entries,
         leader_commit_index: leader_commit_index
       }} ->
        # TODO: Handle an AppendEntryRequest seen by the leader.
        if entries == nil do
          IO.puts("current entries is #{entries}, current term is #{state.current_term}, received term is #{term}")
        end
        if state.current_term < term do
          state = %{state| current_term: term}
          state = %{state| voted_for: nil}
          if entries == nil do
            #heartbeat from new leader
            state = update_leader(state,leader_id)
            IO.puts("old leader receive heartbeat from new leader, change leader and become follower")
            # state = %{state| current_term: term}
            if state.heartbeat_timer != nil do
              IO.puts("cancel leader timer for change to follower")
              Emulation.cancel_timer(state.heartbeat_timer)
            end
            become_follower(state)
          end
          leader(state,extra_state)
        end
        IO.puts(
          "Leader Received append entry for term #{term} with leader #{
            leader_id
          } " <>
            "(#{leader_commit_index})"
        )

      {sender,
       %Raft.AppendEntryResponse{
         term: term,
         log_index: index,
         success: succ
       }} ->
        # TODO: Handle an AppendEntryResposne received by the leader.
        IO.puts("receive message from follower")
        follower_numbers = length(state.view) - 1
        agreement_follower_number = Enum.at(extra_state,index)
        if succ == true && agreement_follower_number <= follower_numbers/2 do
          IO.puts("start update extra_state,index is #{index}")
          extra_state = List.update_at(extra_state,index,&(&1+1))
          IO.puts(["list is ", Enum.join(extra_state, " ")])
          if Enum.at(extra_state,index) > follower_numbers/2 do
            state = apply_log_index(state,index)
            leader(state,extra_state)
          end
          leader(state,extra_state)
        end
        if agreement_follower_number > follower_numbers/2 do
          state = apply_log_index(state,index)
          leader(state,extra_state)
        end
        IO.puts(
          "Received append entry response #{term}," <>
            " index #{index}, succcess #{succ}"
        )
        leader(state,extra_state)

      {sender,
       %Raft.RequestVote{
         term: term,
         candidate_id: candidate,
         last_log_index: last_log_index,
         last_log_term: last_log_term
       }} ->
        # TODO: Handle a RequestVote call at the leader.
        IO.puts("do nothing when leader received vote")
        leader(state,extra_state)

      {sender,
       %Raft.RequestVoteResponse{
         term: term,
         granted: granted
       }} ->
        # TODO: Handle RequestVoteResponse at a leader.
        if term >= state.current_term do
          state = make_follower(state)
          state = %{state | current_term: term}
          state = %{state|voted_for: nil}
          become_follower(state)
        end
        leader(state,extra_state)
        IO.puts(
          "Leader received RequestVoteResponse " <>
            "term = #{term}, granted = #{inspect(granted)}"
        )

      # Messages from external clients. For all of what follows
      # you should send the `sender` an :ok (see `Raft.Client`
      # below) only after the request has completed, i.e., after
      # the log entry corresponding to the request has been **committed**.
      {sender, :nop} ->
        # TODO: entry is the log entry that you need to
        # append.
        entry =
          Raft.LogEntry.nop(
            get_last_log_index(state) + 1,
            state.current_term,
            sender
          )
        prev_log_index = get_last_log_index(state)
        prev_log_term = get_last_log_term(state)
        state = add_log_entries(state,[entry])
        send_AppendEntryRequest = Raft.AppendEntryRequest.new(
          state.current_term,
          whoami(),
          prev_log_index,
          prev_log_term,
          state.log,
          state.commit_index
        )
        broadcast_to_others(state,send_AppendEntryRequest)

        # TODO: You might need to update the following call.
        leader(state, extra_state)

      {sender, {:enq, item}} ->
        # TODO: entry is the log entry that you need to
        # append.
        entry =
          Raft.LogEntry.enqueue(
            get_last_log_index(state) + 1,
            state.current_term,
            sender,
            item
          )

        prev_log_index = get_last_log_index(state)
        prev_log_term = get_last_log_term(state)
        state = add_log_entries(state,[entry])
        send_AppendEntryRequest = Raft.AppendEntryRequest.new(
          state.current_term,
          whoami(),
          prev_log_index,
          prev_log_term,
          state.log,
          state.commit_index
        )
        broadcast_to_others(state,send_AppendEntryRequest)

        # TODO: You might need to update the following call.
        leader(state, extra_state)

      {sender, :deq} ->
        # TODO: entry is the log entry that you need to
        # append.
        entry =
          Raft.LogEntry.dequeue(
            get_last_log_index(state) + 1,
            state.current_term,
            sender
          )
        prev_log_index = get_last_log_index(state)
        prev_log_term = get_last_log_term(state)
        state = add_log_entries(state,[entry])
        send_AppendEntryRequest = Raft.AppendEntryRequest.new(
          state.current_term,
          whoami(),
          prev_log_index,
          prev_log_term,
          state.log,
          state.commit_index
        )
        broadcast_to_others(state,send_AppendEntryRequest)
        # TODO: You might need to update the following call.
        leader(state, extra_state)

      # Messages for debugging [Do not modify existing ones,
      # but feel free to add new ones.]
      {sender, :send_state} ->
        send(sender, state.queue)
        leader(state, extra_state)

      {sender, :send_log} ->
        send(sender, state.log)
        leader(state, extra_state)

      {sender, :whois_leader} ->
        send(sender, {whoami(), state.current_term})
        leader(state, extra_state)

      {sender, :current_process_type} ->
        send(sender, :leader)
        leader(state, extra_state)

      {sender, {:set_election_timeout, min, max}} ->
        send(sender, :ok)

        leader(
          %{state | min_election_timeout: min, max_election_timeout: max},
          extra_state
        )

      {sender, {:set_heartbeat_timeout, timeout}} ->
        state = %{state | heartbeat_timeout: timeout}
        state = reset_heartbeat_timer(state)
        send(sender, :ok)
        leader(state, extra_state)
    end
  end

  @doc """
  This function transitions a process to candidate.
  """
  @spec become_candidate(%Raft{is_leader: false}) :: no_return()
  def become_candidate(state) do
    # TODO:   Send out any messages that need to be sent out
    # you might need to update the call to candidate below.
    # vote for self, so set extra state as 1
    extra_state = 1
    candidate(state,extra_state)
  end

  @doc """
  This function implements the state machine for a process
  that is currently a candidate.

  `extra_state` can be used to store any additional information
  required, e.g., to count the number of votes received.
  """
  @spec candidate(%Raft{is_leader: false}, any()) :: no_return()
  def candidate(state, extra_state) do
    receive do
      # election timeout, become candidate again
      :timer ->
        IO.puts("this election has been failed, candidate #{whoami()} become new candidate")
        # reset election timer
        state = reset_election_timer(state)
        # increment currentTerm
        new_current_term = state.current_term + 1
        state = %{state | current_term: new_current_term}
        # vote for self
        state = %{state | voted_for: whoami()}
        #broad cast others for requestVote
        last_log_index = get_last_log_index(state)
        last_log_term = get_last_log_term(state)
        send_RequestVote = Raft.RequestVote.new(state.current_term,whoami(),last_log_index,last_log_term)
        broadcast_to_others(state,send_RequestVote)
        become_candidate(state)
      {sender,
       %Raft.AppendEntryRequest{
         term: term,
         leader_id: leader_id,
         prev_log_index: prev_log_index,
         prev_log_term: prev_log_term,
         entries: entries,
         leader_commit_index: leader_commit_index
       }} ->
        # TODO: Handle an AppendEntryRequest as a candidate
        # choose new leader as leader
        IO.puts("candidate #{whoami()} received message from new leader #{sender}")
        state = update_leader(state,sender)
        # start working as follower
        if state.current_term > term do
          IO.puts("current term bigger than request term, return notok")
          send(sender,:notok)
        end
        if entries == nil do
          state = %{state| current_term: term}
          if state.election_timer != nil do
            IO.puts("cancel leader timer for change to follower")
            Emulation.cancel_timer(state.election_timer)
          end
          state = %{state|voted_for: nil}
          become_follower(state)
        end
        if  get_log_entry(state,prev_log_index) != :noentry && get_log_entry(state,prev_log_index).term != prev_log_term do
          IO.puts("log[prev_log_index].term != prev_log_term return :notok")
          send(sender,:notok)
        else
          IO.puts("compare the index and term from prev_log_index find the first difference index")
          first_diff_index = compare_log_entries(state,entries,prev_log_index+1)
          IO.puts("first diff index is#{first_diff_index}")
          state = add_log_entries(truncate_log_at_index(state,first_diff_index),truncate_entries_at_index(entries,first_diff_index))
          IO.puts("log len is #{length(state.log)}")
          if leader_commit_index > state.commit_index do
            commit_index_need_follower = min(leader_commit_index,get_last_log_index(state))
            IO.puts("commit_index_follower is #{commit_index_need_follower}")
            state = apply_log_index(state,commit_index_need_follower)
            #state = %{state | commit_index: commit_index_need_follower}
            send_appendResponse = Raft.AppendEntryResponse.new(
              state.current_term,
              get_last_log_index(state),
              true
            )
            IO.puts("start send response to leader")
            send(sender,send_appendResponse)
            # convert to follower
            state = make_follower(state)
            state = %{state|voted_for: nil}
            follower(state,extra_state)
          end

          #state = %{state | commit_index: commit_index_need_follower}
          send_appendResponse = Raft.AppendEntryResponse.new(
            state.current_term,
            get_last_log_index(state),
            true
          )
          IO.puts("start send response to leader")
          send(sender,send_appendResponse)
          follower(state,extra_state)
        end
        IO.puts(
          "Candidate received append entry for term #{term} " <>
            "with leader #{leader_id} " <>
            "(#{leader_commit_index})"
        )

      {sender,
       %Raft.AppendEntryResponse{
         term: term,
         log_index: index,
         success: succ
       }} ->
        # TODO: Handle an append entry response as a candidate
        IO.puts("candidate received AppendEntryResponse, check term to determin stay cand or follower")
        if term >= state.current_term do
          state = make_follower(state)
          state = %{state | current_term: term}
          state = %{state|voted_for: nil}
          become_follower(state)
        end
        candidate(state,extra_state)
        IO.puts(
          "Candidate received append entry response #{term}," <>
            " index #{index}, succcess #{succ}"
        )

      {sender,
       %Raft.RequestVote{
         term: term,
         candidate_id: candidate,
         last_log_index: last_log_index,
         last_log_term: last_log_term
       }} ->
        # TODO: Handle a RequestVote response as a candidate.
        IO.puts("candidate received RequestVote, check term to determin stay cand or follower")
        if term > state.current_term do
          state = make_follower(state)
          state = %{state | current_term: term}
          state = %{state|voted_for: nil}
          become_follower(state)
        end
        candidate(state,extra_state)
        IO.puts(
          "Candidate received RequestVote " <>
            "term = #{term}, candidate = #{candidate}"
        )

      {sender,
       %Raft.RequestVoteResponse{
         term: term,
         granted: granted
       }} ->
        # TODO: Handle a RequestVoteResposne as a candidate.
        if granted == true && state.current_term >= term do
          extra_state = extra_state + 1
          follower_numbers = length(state.view) - 1
          if extra_state >= follower_numbers/2 do
            IO.puts("get more than 1/2 vote, change to leader")
            send_AppendEntryRequest = Raft.AppendEntryRequest.new(
              state.current_term,
              whoami(),
              1,
              1,
              nil,
              1
            )
            IO.puts("send init heartbeat to others")
            broadcast_to_others(state,send_AppendEntryRequest)
            become_leader(state)
          end
        end
        IO.puts(
          "Candidate received RequestVoteResponse " <>
            "term = #{term}, granted = #{inspect(granted)}"
        )



      # Messages from external clients.
      {sender, :nop} ->
        # Redirect in hopes that the current process
        # eventually gets elected leader.
        send(sender, {:redirect, whoami()})
        candidate(state, extra_state)

      {sender, {:enq, item}} ->
        # Redirect in hopes that the current process
        # eventually gets elected leader.
        send(sender, {:redirect, whoami()})
        candidate(state, extra_state)

      {sender, :deq} ->
        # Redirect in hopes that the current process
        # eventually gets elected leader.
        send(sender, {:redirect, whoami()})
        candidate(state, extra_state)

      # Messages for debugging [Do not modify existing ones,
      # but feel free to add new ones.]
      {sender, :send_state} ->
        send(sender, state.queue)
        candidate(state, extra_state)

      {sender, :send_log} ->
        send(sender, state.log)
        candidate(state, extra_state)

      {sender, :whois_leader} ->
        send(sender, {:candidate, state.current_term})
        candidate(state, extra_state)

      {sender, :current_process_type} ->
        send(sender, :candidate)
        candidate(state, extra_state)

      {sender, {:set_election_timeout, min, max}} ->
        state = %{state | min_election_timeout: min, max_election_timeout: max}
        state = reset_election_timer(state)
        send(sender, :ok)
        candidate(state, extra_state)

      {sender, {:set_heartbeat_timeout, timeout}} ->
        send(sender, :ok)
        candidate(%{state | heartbeat_timeout: timeout}, extra_state)
    end
  end
end

defmodule Raft.Client do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @moduledoc """
  A client that can be used to connect and send
  requests to the RSM.
  """
  alias __MODULE__
  @enforce_keys [:leader]
  defstruct(leader: nil)

  @doc """
  Construct a new Raft Client. This takes an ID of
  any process that is in the RSM. We rely on
  redirect messages to find the correct leader.
  """
  @spec new_client(atom()) :: %Client{leader: atom()}
  def new_client(member) do
    %Client{leader: member}
  end

  @doc """
  Send a nop request to the RSM.
  """
  @spec nop(%Client{}) :: {:ok, %Client{}}
  def nop(client) do
    leader = client.leader
    send(leader, :nop)

    receive do
      {_, {:redirect, new_leader}} ->
        nop(%{client | leader: new_leader})

      {_, :ok} ->
        {:ok, client}
    end
  end

  @doc """
  Send a dequeue request to the RSM.
  """
  @spec deq(%Client{}) :: {:empty | {:value, any()}, %Client{}}
  def deq(client) do
    leader = client.leader
    send(leader, :deq)

    receive do
      {_, {:redirect, new_leader}} ->
        deq(%{client | leader: new_leader})

      {_, v} ->
        {v, client}
    end
  end

  @doc """
  Send an enqueue request to the RSM.
  """
  @spec enq(%Client{}, any()) :: {:ok, %Client{}}
  def enq(client, item) do
    leader = client.leader
    send(leader, {:enq, item})

    receive do
      {_, :ok} ->
        {:ok, client}

      {_, {:redirect, new_leader}} ->
        enq(%{client | leader: new_leader}, item)
    end
  end
end
