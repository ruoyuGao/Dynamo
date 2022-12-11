defmodule Dynamo do
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
  defstruct(
    hash_table: nil, #store <key,<value,value_vector_clock,is_replica,hash_code>>
    hash_trees_map: nil, #use to store several hash tree for several key range
    key_range_map: nil, # store <ab: [1,3]>
    message_table: nil,#same as message_list, used for gossip
    prefer_list: nil,
    check_alive_timer: nil, # is much longer than heartbeat timer, in order to check liveness of prefer list
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
      message_table: %{},#{a:1}
      prefer_list: prefer_list, #{:a,:b,:c}
      response_list: [], #{:a,:b,:c}
      key_range_map: key_range_map, #{ab:[1,3], bc:[4,6]}
      heartbeat_timeout: heartbeat_timeout,
      client: client
    }

  end
  @spec virtual_node(%Dynamo{},any)::no_return()
  def virtual_node(state,extra_state) do
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
        hash_tree: hash_tree
      }} ->
        #send GetEntryResponse to sender(2cond: tree is same or not)
        raise "wait to write"
      {sender,%Dynamo.GetEntryResponse{
        key: key,
        value: value,
        is_same: is_same,
        value_vector_clock: value_vector_clock
      }} ->
        #if receive more than R response, send result to client
        raise "wait to write"
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
        raise "wait to write"
      {sender, :heartbeat} ->
        # receiver heartbeat from other nodes, send ok to that node
        send(sender, :alive)
      {sender, :alive} ->
        #receive response from alive nodes in prefer list
        #add the sender in prefer list
        raise "wait to write"
      :check_alive_timer ->
        # check if we have received every response from prefer list
        # if response_list == prefer_list, reset response_list
        # else start gossip
        #send {:gossip, dead_node} to other nodes(except the next node of dead_node)
        #send {:gossip_reconfig, dead_node} to the next node for dead_node
        raise "wait to write"
      {sender,{:gossip_reconfig, dead_node}} ->
        #change all the data in the coordinate key range for dead_node to this node(the previous range of current node coordinate range)
        #notice physical node that coordinate range of dead_node is replaced by current node
        raise "wait to write"
      {sender, {:gossip, dead_node}} ->
        # if dead_node in the message list and not in prefer list, ignore it
        # else delete this node in prefer_list, and add its in message_list
        raise "wait to write"
    end
  end
end

defmodule Dynamo.PhysicalNode do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__
  defstruct(
    node_map: nil,
    )
    @spec physical_node(%PhysicalNode{},any)::no_return()
    def physical_node(state,extra_state) do
      receive do
        {sender, {:get, key}} ->
          # transfer message to virtual node
          raise "wait to write"
        {sender, {:put, key, value, hash_code}} ->
          # transfer message to virtual node
          raise "wait to write"
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
