defmodule Dynamo do
@moduledoc """
  An implementation of the Raft consensus protocol.
  """
  # Shouldn't need to spawn anything from this module, but if you do
  # you should add spawn to the imports.
  import Emulation, only: [send: 2, timer: 2, now: 0, whoami: 0, cancel_timer: 1]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  # This allows you to use Elixir's loggers
  # for messages. See
  # https://timber.io/blog/the-ultimate-guide-to-logging-in-elixir/
  # if you are interested in this. Note we currently purge all logs
  # below Info
  require Logger
  defstruct(
    hash_table: nil, #store {key : {value,value_vector_clock,is_replica,hash_code} }
    hash_trees_map: nil, #use to store several hash tree for several key range {virutal node : tree}
    key_range_map: nil, # store <a: [1,3]> {coordinate vitual node : hashes of key range []}
    # node_coordinate_keys: nil, # store node and their current coordinating keys {a: [1k, 3k]}
    message_list: nil,#same as message_list, used for gossip
    prefer_list: nil,
    check_alive_timer: nil, # is much longer than heartbeat timer, in order to check liveness of prefer list
    check_alive_timeout: nil,
    response_list: nil, #store which node(in prefer_list)has send heartbeat response
    heartbeat_timer: nil,
    heartbeat_timeout: nil,
    put_client: nil, # marks the client sending put request to dynamo
    get_client: nil, # marks the client sending get request to dynamo
    response_cash_map: nil,
    response_w_count_map: nil,
    response_r_count_map: nil,
    w: nil,
    r: nil,
    n: nil
  )
  @spec new_configuration(
    [atom()],
    %{},
    non_neg_integer(),
    non_neg_integer(),
    non_neg_integer(),
    non_neg_integer()
  ):: %Dynamo{}
  def new_configuration(
    prefer_list,
    key_range_map,
    heartbeat_timeout,
    w,
    r,
    n
  ) do
    %Dynamo{
      hash_table: Map.new(),
      hash_trees_map: %{},#{a: 1, b: 1, c:1}
      message_list: [], #{:a}
      prefer_list: prefer_list, #{:a,:b,:c}
      response_list: [], #{:a,:b,:c}
      key_range_map: key_range_map, #{a:[1,3], b:[4,6]}
      heartbeat_timeout: heartbeat_timeout,
      response_cash_map: %{},#store cash for return message to clinet
      response_w_count_map: %{},
      response_r_count_map: %{},
      w: w,
      r: r,
      n: n
    }

  end

  @spec convert_to_hash_list(map())::list()
  def convert_to_hash_list(hash_table) do
    Enum.map(hash_table,fn({key,objectEntry}) -> {objectEntry.hash_code}end)
  end
  @spec find_key_value_from_hash(%Dynamo{}, non_neg_integer())::{any(),%Dynamo.ObjectEntry{}}
  def find_key_value_from_hash(state,hash_code) do
    obj = state.hash_table |> Enum.find(fn {key, val} -> val.value == hash_code end)
    obj
    # the value hash is it self for test case
    #{obj_key, state.hash_table[obj_key]}
  end

  ###########     Utility Functions Starts     ##########
  # Save a handle to the hearbeat timer.
  @spec save_heartbeat_timer(%Dynamo{}, reference()) :: %Dynamo{}
  defp save_heartbeat_timer(state, timer) do
    %{state | heartbeat_timer: timer}
  end

  @spec reset_heartbeat_timer(%Dynamo{}) :: %Dynamo{}
  defp reset_heartbeat_timer(state) do
    if state.heartbeat_timer != nil do
      cancel_timer(state.heartbeat_timer)
    end
    new_heartbeat_timer = timer(state.heartbeat_timeout, :heartrbeat_timer)
    save_heartbeat_timer(state, new_heartbeat_timer)
  end

  # Save a handle to the hearbeat timer.
  @spec save_checkalive_timer(%Dynamo{}, reference()) :: %Dynamo{}
  defp save_checkalive_timer(state, timer) do
    %{state | check_alive_timer: timer}
  end

  @spec reset_checkalive_timer(%Dynamo{}) :: %Dynamo{}
  defp reset_checkalive_timer(state) do
    if state.check_alive_timer != nil do
      cancel_timer(state.check_alive_timer)
    end
    new_checkalive_timer = timer(state.check_alive_timeout, :check_alive_timer)
    save_checkalive_timer(state, new_checkalive_timer)
  end

  @spec gossip_send(map(), atom()) :: [any()]
  defp gossip_send(prefer_map, dead) do
    # find the next node of this dead note on the prefer_list, send :gossip_reconfig to it.
    # send messages to all that follows
    dead_idx = prefer_map[dead]
    prefer_map
      |> Enum.map(fn {pid, idx} ->
        if idx == dead_idx + 1 do
          #send {:gossip_reconfig, dead_node} to the next node for dead_node
          send(pid, {:gossip_reconfig, dead})
        else
          #send {:gossip, dead_node} to other nodes(except the next node of dead_node)
          send(pid, {:gossip, dead})
        end
      end)
  end
  # broadcast to prefer list with any message
  @spec broadcast_to_prefer_list(%Dynamo{}, any())::no_return()
  defp broadcast_to_prefer_list(state, message) do
    broadcast_list = Enum.take(state.prefer_list, state.n)
    broadcast_list |> Enum.map(fn pid -> send(pid, message) end)
  end

  # given a list of keys, extract the corresponding values from the hash table
  @spec get_values(%Dynamo{}, [any()]) :: [any()]
  defp get_values(state, keys) do
    values = Enum.map(keys, fn k -> state.hash_table[k].value end)
    values
  end

  # given key range, return all the keys in that range, sorted
  @spec get_keys_from_range(%Dynamo{}, [any()]) :: [any()]
  defp get_keys_from_range(state, key_range) do
    first = hd(key_range)
    last = List.last(key_range)
    # IO.puts("Based on range #{first} and #{last}, find out hash table #{inspect(state.hash_table)}")
    keys = Enum.map(state.hash_table, fn {k, v} -> if v.hash_code >=first and v.hash_code < last do k end end)
    # IO.puts("find out keys #{inspect(keys)}")
    sorted_keys = Enum.sort(keys)
    sorted_keys
  end

  @spec hash_function(String.t()) :: String.t()
  def hash_function(meta_data) do
    result = MerkleTree.Crypto.hash(meta_data,:md5)
    result
  end

  @spec update_hash_table_tree(%Dynamo{}, non_neg_integer(), any(), non_neg_integer(), map(), atom()):: %Dynamo{}
  def update_hash_table_tree(state, key, value, hash_code, value_vector_clock, sender) do
    state =
      if Map.has_key?(state.hash_table, key) do
        if value_vector_clock[sender] >= state.hash_table[key].value_vector_clock[sender] do
          # temp_vector_clock = Map.update!(state.hash_table[key].value_vector_clock, sender, value_vector_clock[sender])
          temp_vector_clock = Map.replace!(state.hash_table[key].value_vector_clock, sender, value_vector_clock[sender])
          temp_entry_obj = state.hash_table[key]
          temp_entry_obj = %{temp_entry_obj | value_vector_clock: temp_vector_clock, value: value, hash_code: hash_code}
          temp_hash_table = Map.replace!(state.hash_table, key, temp_entry_obj)
          %{state | hash_table: temp_hash_table}
        else
          state # do not update
        end
      else
        is_replica = 1
        new_entry_obj = Dynamo.ObjectEntry.putObject(value, value_vector_clock, is_replica, hash_code)
        temp_hash_table = Map.put(state.hash_table, key, new_entry_obj)
        %{state | hash_table: temp_hash_table}
      end
    #use hash_code to search which key range it belongs to, reconstruct the hash_tree
    {obj_key, key_range} = state.key_range_map |> Enum.find(fn {vn, key_range} ->
      if hash_code >= hd(key_range) and hash_code<List.last(key_range) do
        {vn, key_range}
      end
    end)
    # value_list = Enum.map(key_range, fn k -> state.hash_table[k] end)
    # use this value list to build merkel tree
    IO.puts("updating MT for node #{obj_key} and key #{key} value #{value}")
    keys_in_range = get_keys_from_range(state, key_range)
    values_in_range = get_values(state, keys_in_range)
    new_hash_tree = MerkleTree.new(values_in_range, &hash_function/1)
    # temp_hash_tree_map = Map.replace!(state.hash_trees_map, obj_key, new_hash_tree)
    temp_hash_tree_map = Map.put(state.hash_trees_map, obj_key, new_hash_tree) # can be a new tree for the range
    state = %{state | hash_trees_map: temp_hash_tree_map}
  end

  @spec start_MT_Check(%Dynamo{}, non_neg_integer(), list(), atom()) :: any()
  def start_MT_Check(state, key, extra_state, sender) do
    IO.puts("start MT check")
    hash_code = state.hash_table[key].hash_code
    obj_key = state.key_range_map |> Enum.find(fn {key, val} ->
          if hash_code >= hd(val) and hash_code<=List.last(val) do
              {key, val}
          end
        end) |> elem(0)
    hash_tree_root = state.hash_trees_map[obj_key].root()
    extra_state = hash_tree_root.children
    # if the only node is leaf node, direct send updateHashTableRequest
    if List.first(extra_state) == nil do
      hash_code = hash_tree_root.value
      {key,object} = find_key_value_from_hash(state, hash_code)
      new_updateHashTable = Dynamo.UpdateHashTableRequest.new(key,object.value,object.hash_code,object.value_vector_clock)
      send(sender,new_updateHashTable)
    end
    hash_tree_root_left_child = List.first(extra_state)
    send(sender,{:MTCheck, hash_tree_root_left_child})
    {state, extra_state}
  end

  @spec start_MT_check(%Dynamo{} ,non_neg_integer(),atom()) :: no_return()
  def start_MT_check(state, key, sender) do
    IO.puts("start MT check")
    # get necessary message from hash table
    value = state.hash_table[key].value
    hash_code = state.hash_table[key].hash_code
    value_vector_clock =  state.hash_table[key].value_vector_clock
    # find tree for this key
    key_range_id = state.key_range_map |> Enum.find(fn {key, val} ->
      if hash_code >= hd(val) and hash_code<=List.last(val) do
          {key, val}
      end
    end) |> elem(0)
    coordinate_hash_tree = state.hash_trees_map[key_range_id]
    send(sender, {:updateHashTree, {coordinate_hash_tree, key_range_id, key, value, hash_code, value_vector_clock}})
  end
  ##########     Utility Function Ends     ##########

  @spec become_virtual_node(%Dynamo{}) :: no_return()
  def become_virtual_node(state) do
    extra_state = []
    state = reset_heartbeat_timer(state)
    virtual_node(state, extra_state)
  end

  @spec virtual_node(%Dynamo{},any)::no_return()
  def virtual_node(state, extra_state) do
    receive do
      {sender, %Dynamo.PutEntryRequest{
        key: key,
        value: value,
        hash_code: hash_code,
        value_vector_clock: value_vector_clock
      }} ->
        # if vector is earlier than current object vector_clock, do not update
        # else update the object in hash_table
        IO.puts("<<Dynamo>> #{inspect(whoami())} received put entry request from #{inspect(sender)} for key #{key} value #{value}")
        state = update_hash_table_tree(state,key,value, hash_code,value_vector_clock,sender)
        #send PutEntryResponse to coordinate node
        new_putEntryResponse = Dynamo.PutEntryResponse.new(hash_code, true)
        send(sender,new_putEntryResponse)
        virtual_node(state, extra_state)

      {sender, %Dynamo.PutEntryResponse{
        key_value: key_value,
        success: success
      }} ->
        #use hash_code to identify the key_value pair
        # use map in extra state to make sure if get more than W response, return message to client
        IO.puts("<<Dynamo>> #{inspect(whoami())} received put entry response from #{inspect(sender)} for hash #{key_value}")
        state =
          if success do
            prev_count =
            if  Map.has_key?(state.response_w_count_map,key_value) do
              state.response_w_count_map[key_value]
            else
              0
            end
            temp_response_w_count_map = Map.put(state.response_w_count_map,key_value,prev_count+1)
            state = %{state | response_w_count_map: temp_response_w_count_map}
            state =
              if state.response_w_count_map[key_value] > state.w do
                send(state.put_client,:ok)
                temp_response_w_count_map = Map.delete(state.response_w_count_map,key_value)
                %{state | response_w_count_map: temp_response_w_count_map}
                # virtual_node(state,extra_state)
              else
                state
              end
            state
          else
            state
          end
        virtual_node(state,extra_state)

      {sender, %Dynamo.GetEntryRequest{
        key: key,
        hash_tree_root: hash_tree_root
      }} ->
        #calculate all the variable we need
        IO.puts("<<Dynamo>> #{inspect(whoami())} received get entry request from #{inspect(sender)} for key #{key}")
        hash_code = state.hash_table[key].hash_code
        value_vector_clock = state.hash_table[key].value_vector_clock
        value = state.hash_table[key].value
        obj_key = state.key_range_map |> Enum.find(fn {key, val} ->
          if hash_code >= hd(val) and hash_code<=List.last(val) do
            {key, val}
          end
        end) |> elem(0)
        hash_tree_root_replica = state.hash_trees_map[obj_key].root()
        #start GetEntryresponse when root is not same
        if hash_tree_root.value != hash_tree_root_replica.value do
          extra_state = hash_tree_root_replica.children
          is_same = 0
          new_getEntryResponse = Dynamo.GetEntryResponse.new(key, value, is_same, value_vector_clock)
          send(sender,new_getEntryResponse)
          virtual_node(state,extra_state)
        else
          is_same = 1
          new_getEntryResponse = Dynamo.GetEntryResponse.new(key, value, is_same, value_vector_clock)
          send(sender,new_getEntryResponse)
          virtual_node(state,extra_state)
        end
        # virtual_node(state,extra_state)

      {sender,%Dynamo.GetEntryResponse{
        key: key,
        value: value,
        is_same: is_same,
        value_vector_clock: value_vector_clock
      }} ->
        #if receive more than R response, send result to client
        IO.puts("<<Dynamo>> #{inspect(whoami())} received get entry response from #{inspect(sender)} for key #{key}")
        state =
          if Map.has_key?(state.response_cash_map, key) == true do
            IO.puts("current response cash map for key #{key} is #{inspect(state.response_cash_map[key])}")
            IO.puts("current count for key #{key}: #{state.response_r_count_map[key]}")
            temp_response_r_count_map = Map.put(state.response_r_count_map, key, state.response_r_count_map[key]+1)
            state = %{state | response_r_count_map: temp_response_r_count_map}
            state =
              if state.hash_table[key].value != value do
                temp_response_cash_map = Map.replace!(state.response_cash_map, key, state.response_cash_map[key] ++ [{value,value_vector_clock}])
                %{state | response_cash_map: temp_response_cash_map}
              else
                state
              end
            state =
              if state.response_r_count_map[key] > state.r do
                send(state.get_client, {:value, state.response_cash_map[key]})
                temp_response_r_count_map = Map.delete(state.response_r_count_map,key)
                temp_response_cash_map = Map.delete(state.response_cash_map,key)
                %{state | response_cash_map: temp_response_cash_map, response_r_count_map: temp_response_r_count_map}
              else
                state
              end
            state
          else
            IO.puts("response can not find the key in cash, so it may have some problem")
            state
          end
        # if is not same , start MTCHeck for the first time, put root node in extra state
        if is_same == 0 do
          #Start MTCheck
          #{state, extra_state} = start_MT_Check(state,key, extra_state,sender)
          start_MT_check(state, key, sender) # skip the tree check
          # virtual_node(state, extra_state)
        end
        virtual_node(state, extra_state)

      {sender,{:MTCheckResponse, node_is_same}} ->
        {checked_node,extra_state} = List.pop_at(extra_state, 0)
        if node_is_same == 0 do
          if length(checked_node.children) == 0 do
            hash_code = checked_node.value
            {key,object} = find_key_value_from_hash(state, hash_code)
            new_updateHashTable = Dynamo.UpdateHashTableRequest.new(key,object.value,object.hash_code,object.value_vector_clock)
            send(sender,new_updateHashTable)
          else
            extra_state = extra_state ++ checked_node.children
          end
        end
        head = List.first(extra_state)
        send(sender,{:MTCheck, head})
        virtual_node(state,extra_state)

      {sender, {:MTCheck, tree_node}} ->
        {head,extra_state} = List.pop_at(extra_state, 0)
        node_is_same = 1
        if head.value != tree_node.value do
          node_is_same = 0
          extra_state = extra_state ++ tree_node.children
          send(sender,{:MTCheckResponse, node_is_same})
          virtual_node(state,extra_state)
        else
          send(sender,{:MTCheckResponse, node_is_same})
          virtual_node(state,extra_state)
        end

      {sender, {:get, key}} ->
        #receive get request from client
        #if the node store this key and it is not replica, broadcast GetEntryRequest
        #else transfer this message to the next node in prefer list
        #save first kind of value of key
        # IO.puts("<<Dynamo>> #{inspect(whoami())} received get from client #{inspect(sender)} for key #{key}")
        #if coordinate node not save this key return nil
        current_process = whoami()
        state = %{state | get_client: sender} # update get client, prepare for response

        if Map.has_key?(state.hash_table, key) do
          is_replica = state.hash_table[key].is_replica
          if is_replica == 0 do
            IO.puts("<<Dynamo>> Get: node has key, and is not replica, dealing with it")
            value_vector_clock = state.hash_table[key].value_vector_clock
            value = state.hash_table[key].value
            hash_code = state.hash_table[key].hash_code
            obj_key = state.key_range_map |> Enum.find(fn {key, val} ->
              if hash_code >= hd(val) and hash_code < List.last(val) do
                {key, val}
              end
            end) |> elem(0)
            hash_tree_root = state.hash_trees_map[obj_key].root()
            # broadcast GetEntryRequest, NOTE put earlier
            new_getEntryRequest = Dynamo.GetEntryRequest.new(key, hash_tree_root)
            broadcast_to_prefer_list(state, new_getEntryRequest)
            # initialize for read counting
            temp_cash_count_map = Map.put(state.response_r_count_map, key, 1)
            temp_cash_map = Map.put(state.response_cash_map, key, [{value,value_vector_clock}])
            state = %{state | response_r_count_map: temp_cash_count_map}
            state = %{state | response_cash_map: temp_cash_map}
            IO.puts("<<Dynamo>> #{inspect(current_process)} initialize counter for key #{key} as #{state.response_r_count_map[key]}")
            # # broadcast GetEntryRequest, TODO: put earlier?
            # new_getEntryRequest = Dynamo.GetEntryRequest.new(key, hash_tree_root)
            # broadcast_to_prefer_list(state, new_getEntryRequest)
            virtual_node(state,extra_state)
          else
            transfer_to = hd(state.prefer_list)
            IO.puts("<<Dynamo>> node has key, but node is replica for the key #{key}, transfer to #{transfer_to}")
            send(transfer_to, {:get, key})
            virtual_node(state, extra_state)
          end
        else
          if hd(state.key_range_map[current_process])<= key and key < List.last(state.key_range_map[current_process]) do
            # IO.puts("<<Dynamo>> #{inspect(whoami())} fail to get #{key} with coordinate node")
            send(state.get_client, :nil)
          else
            transfer_to = hd(state.prefer_list)
            IO.puts("<<Dynamo>> #{inspect(whoami())} transfer get key #{key} to #{transfer_to}")
            send(transfer_to, {:get, key})
          end
          virtual_node(state, extra_state)
        end

      {sender, {:put, key, value, hash_code}} ->
        #coordinate node receive put request from client, broadcast PutEntryRequest to prefer list
        is_replica = 0
        # create value vector clock
        current_proc = whoami()
        state = %{state | put_client: sender}
        IO.puts("<<Dynamo>> #{inspect(current_proc)} received put from client #{inspect(sender)} for key #{key} value #{value}")
        value_vector_clock =
          if Map.has_key?(state.hash_table, key) do
            temp_vector_clock_map = state.hash_table[key].value_vector_clock
            if Map.has_key?(temp_vector_clock_map, current_proc) do
              Map.update!(temp_vector_clock_map, current_proc, &(&1 + 1))
            else
              Map.put_new(temp_vector_clock_map, current_proc, 1)
            end
          else
            temp_clock = Map.new()
            Map.put_new(temp_clock, current_proc, 1)
          end
        new_entry_obj = Dynamo.ObjectEntry.putObject(value, value_vector_clock, is_replica, hash_code)
        temp_hash_table = Map.put(state.hash_table, key, new_entry_obj)
        state =
          if Map.has_key?(state.response_cash_map, key) do
            # if client is waiting for the get() for this key, we should also update the value here for an up-to-date response
            # the count does not change, because the count only depend on how many nodes have received the get request,
            # and this node already does.
            temp_cash_map = Map.replace!(state.response_cash_map, key, [{value,value_vector_clock}])
            %{state | response_cash_map: temp_cash_map}
          else
            state
          end
        state = %{state | hash_table: temp_hash_table}
        #reconstruct hash tree
        #use hash_code to search which key range it belongs to, reconstruct the hash_tree
        {obj_key, key_range} = state.key_range_map |> Enum.find(fn {vn, key_range} ->
          if hash_code >= hd(key_range) and hash_code<List.last(key_range) do
            {vn, key_range}
          end
        end)
        # value_list = Enum.map(key_range, fn k -> state.hash_table[k] end)
        # use this value list to build merkel tree
        # IO.puts("node #{obj_key} and key range #{inspect(key_range)}")
        keys_in_range = get_keys_from_range(state, key_range)
        values_in_range = get_values(state, keys_in_range)
        # IO.puts("current hast table #{inspect(state.hash_table)}")
        # IO.puts("keys in range #{keys_in_range}, values in range #{values_in_range}")
        new_hash_tree = MerkleTree.new(values_in_range, &hash_function/1)
        temp_hash_tree_map = Map.put(state.hash_trees_map, obj_key, new_hash_tree) # obj can refer to a new tree
        state = %{state | hash_trees_map: temp_hash_tree_map}
        #broadcast PutentryRequest
        new_putEntryRequest = Dynamo.PutEntryRequest.new(key, value, hash_code, value_vector_clock)
        broadcast_to_prefer_list(state, new_putEntryRequest)
        send(sender, :ok)
        virtual_node(state,extra_state)

      :heartbeat_timer ->
        #When heartbeat is timeout, reset timer and broadcast to prefer list
        state.prefer_list
          |> Enum.map(fn pid -> send(pid, :heartbeat) end)
        # reset heartbeat timer
        state = reset_heartbeat_timer(state)
        virtual_node(state, extra_state)

      {sender, :heartbeat} ->
        # receiver heartbeat from other nodes, send ok to that node
        send(sender, :alive)
        virtual_node(state, extra_state)

      {sender, :alive} ->
        #receive response from alive nodes in prefer list
        #add the sender in prefer list
        state = %{state | response_list: state.response_list ++ [sender] }
        virtual_node(state, extra_state)

      :check_alive_timer ->
        # check if we have received every response from prefer list
        # if response_list == prefer_list, reset response_list
        # else start gossip
        #send {:gossip, dead_node} to other nodes(except the next node of dead_node)
        #send {:gossip_reconfig, dead_node} to the next node for dead_node
        unique_response_list = Enum.uniq(state.resonse_list) # keep every alive node only once
        diff = state.prefer_list -- unique_response_list
        indexed_prefer_list = Enum.with_index(state.prefer_list)
        prefer_map = indexed_prefer_list
                      |> Map.new(fn {pid, idx} -> {pid, idx} end)
        diff
          |> Enum.map(fn dead -> gossip_send(prefer_map, dead) end)
        state = %{state | response_list: []}
        state = reset_checkalive_timer(state)
        virtual_node(state, extra_state)

      {sender,{:gossip_reconfig, dead_node}} ->
        #change all the data in the coordinate key range for dead_node to this node(the previous range of current node coordinate range)
        #notice physical node that coordinate range of dead_node is replaced by current node
        dead_key_range = state.key_range_map[dead_node] # is hash
        first = hd(dead_key_range)
        last = List.last(dead_key_range)
        reconfiged_hash_table =
            state.hash_table |> Enum.map(fn {k, obj} ->
              if obj.hash_code >= first and obj.hash_code < last do
                {k, %{obj | is_replica: False}}
              else
                {k, obj}
              end
            end)
        state = %{state | hash_table: reconfiged_hash_table}
        virtual_node(state, extra_state)

      {sender, {:gossip, dead_node}} ->
        # if dead_node in the message list and not in prefer list, ignore it
        # else delete this node in prefer_list, and add its in message_list
        state =
          if Enum.member?(state.prefer_list, dead_node) do
            %{state | prefer_list: List.delete(state.prefer_list, dead_node)}
          else
            state
          end
        state =
          if not Enum.member?(state.message_list, dead_node) do
            %{state | message_list: state.message_list ++ [dead_node]}
          else
            state
          end
        virtual_node(state, extra_state)

      {sender, %Dynamo.UpdateHashTableRequest{
        key: key,
        value: value,
        hash_code: hash_code,
        value_vector_clock: value_vector_clock
      }} ->
        #update hash table and hash node
        state = update_hash_table_tree(state,key,value, hash_code,value_vector_clock,sender)
        virtual_node(state, extra_state)

      {sender, {:updateHashTree, {coordinate_hash_tree, key_range_id, key, value, hash_code, value_vector_clock}}} ->
        #update hash table
        is_replica = 1
        new_entry_obj = Dynamo.ObjectEntry.putObject(value, value_vector_clock, is_replica, hash_code)
        temp_hash_table = Map.put(state.hash_table, key, new_entry_obj)
        state = %{state | hash_table: temp_hash_table}
        # update hash tree
        temp_hash_tree_map = Map.put(state.hash_trees_map, key_range_id, coordinate_hash_tree)
        state = %{state | hash_trees_map: temp_hash_tree_map}
        virtual_node(state, extra_state)
    end
  end
end

defmodule Dynamo.PhysicalNode do
  alias Dynamo.PhysicalNode
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__
  defstruct(node_map: nil) # {virtual node key => [key range list]}

  @spec new_physical_node(map()) :: %PhysicalNode{}
  def new_physical_node(node_map) do
    %PhysicalNode{node_map: node_map}
  end

  @spec physical_node(%PhysicalNode{},any)::no_return()
  def physical_node(state,extra_state) do
    receive do
      {sender, {:get, key}} ->
        # transfer message to virtual node
        virtual_nodes = Map.keys(state.node_map)
        target = hd(virtual_nodes) # pick the first one to send
        send(target, {:get, key})
        physical_node(state,extra_state)

      {sender, {:put, key, value, hash_code}} ->
        # transfer message to virtual node
        found =
          state.node_map |> Enum.filter(fn {vn, range} -> hash_code >= hd(range) and hash_code < List.last(range) end)
        # node_map[virtual_node].first()
        if length(found)>0 do
          {vn, rg} = hd(found)
          send(vn, {:put, key, value, hash_code})
        else
          IO.puts("Warning! range not found in physical node: #{whoami()}'s node_map list!")
        end
        physical_node(state,extra_state)
    end

  end

end

defmodule Dynamo.Client do
  import Emulation, only: [send: 2, timer: 2, cancel_timer: 1]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__
  defstruct(
    node_list: nil
    )
  #node_list store node name and its position on the ring
  @spec new_client(list(any())) :: %Client{node_list: list(any())}
  def new_client(node_list) do
    %Client{node_list: node_list}
  end

  @spec put(%Client{},non_neg_integer(), any())::{:ok, %Client{}}
  def put(client, key, value) do
    hash_code = key #+ value # use hash function later
    #check node_map to find coordinate node
    #send put to coordinate node
    IO.puts("**Client** sending new data key #{key}, value #{value}")
    coordinate = hd(client.node_list)
    send(coordinate, {:put, key, value, hash_code})
    receive do
      {_, :ok} ->
        {:ok, client}
    end
  end

  @spec get(%Client{},non_neg_integer())::{{:value, non_neg_integer()}, %Client{}}
  def get(client, key) do
    #send get request to physical node
    coordinate = hd(client.node_list)
    send(coordinate, {:get, key})
    receive do
      {_, value} ->
        IO.puts("values got: #{inspect(value)}")
        {{:value, value},client}
      end
  end

  # keep sending get messges until get one, trigged after sending out the first send
  @spec keep_get(pid(), non_neg_integer()) :: {{:value, list()}, any()}
  defp keep_get(coordinate, key) do
    receive do
      {_, :nil} ->
        # IO.puts("**Client** read empty, keep sending get")
        # current_time = System.monotonic_time()
        # IO.puts("System time is #{inspect(current_time)}")
        send(coordinate, {:get, key})
        keep_get(coordinate, key)
      {_, :ok} ->
        IO.puts("**Client** got a put success from Dynamo, keep waiting")
        keep_get(coordinate, key)
      {_, {:value, values}} ->
        # IO.puts("got: #{inspect(values)}")
        # {value, vector_clock} = hd(values)
        # IO.puts("first value got: #{inspect(value)}")
        # IO.puts("first value got: #{inspect(vector_clock)}")
        get_time = System.monotonic_time()
        {{:value, values}, get_time}
    end
  end

  @spec put_and_get(%Client{}, non_neg_integer(), any())::{{:value, list()}, %Client{}}
  def put_and_get(client, key, value) do
    hash_code = key
    coordinate = hd(client.node_list)
    IO.puts("**Client** sending new data key #{key}, value #{value}")
    send(coordinate, {:put, key, value, hash_code})
    start_time = System.monotonic_time()
    # IO.puts("System time is #{inspect(start_time)}")
    send(coordinate, {:get, key})
    {{:value, values}, get_time} = keep_get(coordinate, key)
    # IO.puts("System time when successfully get is #{inspect(get_time)}")
    take_time = System.convert_time_unit(get_time - start_time, :native, :millisecond)
    IO.puts("time for visibility #{inspect(take_time)} ms")
    {{:value, values}, client}
  end

  # get and listen
  # this function keeps sending out get() and listens to the response
  @spec get_and_listen(%Client{}, non_neg_integer()) :: {{:value, list()}, %Client{}}
  def get_and_listen(client, key) do
    coordinate = hd(client.node_list)
    start_time = System.monotonic_time()
    send(coordinate, {:get, key})
    {{:value, values}, get_time} = keep_get(coordinate, key)
    take_time = System.convert_time_unit(get_time - start_time, :native, :millisecond)
    IO.puts("time for visibility #{inspect(take_time)} ms")
    {{:value, values}, client}
  end

  # periodically send out get request, record and return the response number of versions
  @spec periodical_get(%Client{}, non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer(), [non_neg_integer()]) :: [non_neg_integer()]
  def periodical_get(client, interval, interval_elipsed, num_trials, key, record) do
    if num_trials == length(record) do
      record
    else
      if interval_elipsed == 0 do
        IO.puts("**get client** next interval: send get")
        coordinate = hd(client.node_list)
        send(coordinate, {:get, key})
      end
      time_left = interval - interval_elipsed
      intv_timer = timer(time_left, :intv_timer)
      # intv_timer = timer(interval - interval_left, :intv_timer)
      receive do
        :intv_timer ->
          IO.puts("time end")
          periodical_get(client, interval, 0, num_trials, key, record)
        {_, :nil} ->
          IO.puts("get nil")
          # record ++ [0] # read 0 version
          time_left = cancel_timer(intv_timer)
          periodical_get(client, interval, interval-time_left, num_trials, key, record ++ [0])
        {_, {:value, values}} ->
          IO.puts("get values")
          # record ++ [length(values)]
          time_left = cancel_timer(intv_timer)
          periodical_get(client, interval, interval-time_left, num_trials, key, record ++ [length(values)])
        {_, anything} ->
          IO.puts("get #{inspect(anything)}")
      end

    end
  end
end
