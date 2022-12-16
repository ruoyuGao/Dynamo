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
    hash_trees_map: nil, #use to store several hash tree for several key range
    key_range_map: nil, # store <ab: [1,3]>
    message_list: nil,#same as message_list, used for gossip
    prefer_list: nil,
    check_alive_timer: nil, # is much longer than heartbeat timer, in order to check liveness of prefer list
    check_alive_timeout: nil,
    response_list: nil, #store which node(in prefer_list)has send heartbeat response
    heartbeat_timer: nil,
    heartbeat_timeout: nil,
    client: nil
  )
  @spec new_configuration(
    [atom()],
    %{},
    non_neg_integer(),
    atom()
  ):: %Dynamo{}
  def new_configuration(
    prefer_list,
    key_range_map,
    heartbeat_timeout,
    client
  ) do
    %Dynamo{
      hash_table: Map.new(),
      hash_trees_map: %{},#{a: 1, b: 1, c:1}
      message_list: [], #{:a}
      prefer_list: prefer_list, #{:a,:b,:c}
      response_list: [], #{:a,:b,:c}
      key_range_map: key_range_map, #{a:[1,3], b:[4,6]}
      heartbeat_timeout: heartbeat_timeout,
      client: client
    }

  end

  @spec convert_to_hash_list(map())::list()
  def convert_to_hash_list(hash_table) do
    Enum.map(hash_table,fn({key,objectEntry}) -> {objectEntry.hash_code}end)
  end
  @spec find_key_value_from_hash(%Dynamo{}, non_neg_integer())::{any(),%Dynamo.ObjectEntry{}}
  def find_key_value_from_hash(state,hash_code) do
    obj = state.hash_table |> Enum.find(fn {key, val} -> val.hash_code == hash_code end)
    obj
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
    if state.hearbeat_timer != nil do
      cancel_timer(state.heatbeat_timer)
    end
    new_heatbeat_timer = timer(state.heatbeat_timeout, :heatrbeat_timer)
    save_heartbeat_timer(state, new_heatbeat_timer)
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

  @spec gossip_send(%Dynamo{}, map(), atom()) :: any()
  defp gossip_send(state, prefer_map, dead) do
    # find the next node of this dead note on the prefer_list
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
    state.prefer_list |> Enum.map(fn pid -> send(pid, message) end)
  end

  ##########     Utility Function Ends     ##########

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

        #use hash_code to search which key range it belongs to, reconstruct the hash_tree

        #send PutEntryResponse to coordinate node
        "wait to write"
      {sender, %Dynamo.PutEntryResponse{
        hash_code: hash_code,
        success: success
      }} ->
        #use hash_code to identify the key_value pair
        # use map in extra state to make sure if get more than W response, return message to client
        raise "wait to write"
      {sender, %Dynamo.GetEntryRequest{
        key: key,
        hash_tree_root: hash_tree_root
      }} ->
        #send GetEntryResponse to sender(2cond: tree is same or not)
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
          broadcast_to_prefer_list(state,new_getEntryResponse)
        else
          is_same = 1
          new_getEntryResponse = Dynamo.GetEntryResponse.new(key, value, is_same, value_vector_clock)
          broadcast_to_prefer_list(state,new_getEntryResponse)
        end
        virtual_node(state,extra_state)
      {sender,%Dynamo.GetEntryResponse{
        key: key,
        value: value,
        is_same: is_same,
        value_vector_clock: value_vector_clock
      }} ->
        #if receive more than R response, send result to client
        # if is not same , start MTCHeck for the first time, put root node in extra state
        if is_same == 0 do
          #Start MTCheck
          hash_code = state.hash_table[key].hash_code
          obj_key = state.key_range_map |> Enum.find(fn {key, val} ->
            if hash_code >= hd(val) and hash_code<=List.last(val) do
              {key, val}
            end
          end) |> elem(0)
          hash_tree_root = state.hash_trees_map[obj_key].root()
          extra_state = hash_tree_root.children
          hash_tree_root_left_child = List.first(extra_state)
          send(sender,{:MTCheck, hash_tree_root_left_child})
          virtual_node(state,extra_state)
        end
        raise "wait to write"
      {sender,{:MTCheckResponse, node_is_same}} ->
        {checked_node,extra_state} = List.pop_at(extra_state, 0)
        if node_is_same == 0 do
          if length(checked_node.children) == 0 do
            hash_code = checked_node.value
            {key,object} = find_key_value_from_hash(state, hash_code)
            new_updateHashTable = Dynamo.UpdateHashTableRequest.new(key,object.value,hash_code,object.value_vector_clock)
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
        raise "wait to write"
      {sender, {:put, key, value, hash_code}} ->
        #coordinate node receive put request from client, broadcast PutEntryRequest to prefer list
        raise "wait to write"

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
        diff = state.prefer_list -- state.response_list
        indexed_prefer_list = Enum.with_index(state.prefer_list)
        prefer_map = indexed_prefer_list
                      |> Map.new(fn {pid, idx} -> {pid, idx} end)
        diff
          |> Enum.map(fn dead -> gossip_send(state, prefer_map, dead) end)
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
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__
  defstruct(
    node_list: nil,
    )
  #node_list store node name and its position on the ring
  @spec new_client(list(any())) :: %Client{node_list: list(any())}
  def new_client(node_list) do
    %Client{node_list: node_list}
  end

  @spec put(%Client{},non_neg_integer(),non_neg_integer())::{:ok, %Client{}}
  def put(client, key, value) do
    hash_code = key + value # use hash function later
    #check node_map to find coordinate node
    #send put to coordinate node
    receive do
      {_, :ok} ->
        {:ok, client}
    end
  end

  @spec get(%Client{},non_neg_integer())::{:ok,{non_neg_integer(), %Client{}}}
  def get(client, key) do
    #send get request to physical node
    receive do
      {_, value} ->
        {value,client}
      end
  end
end
