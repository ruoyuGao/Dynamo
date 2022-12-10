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
  @spec hash_function(String.t()) :: String.t()
  def hash_function(meta_data) do
    result = MerkleTree.Crypto.hash(meta_data,:md5)
    result
  end

  defstruct(
    hash_table: nil,
    hash_tree: nil,
    prefer_list: nil,
    vector_clock: nil, #same as message_list
    heartbeat_timer: nil,
    heartbeat_timeout: nil
  )
  @spec new_configuration(
    [atom()],
    non_neg_integer()
  ):: %Dynamo{}
  def new_configuration(
    prefer_list,
    heartbeat_timeout
  ) do
    %Dynamo{
      hash_table: Map.new(),
      hash_tree: MerkleTree.new(["placeholder"],&hash_function/1),
      prefer_list: [],
      vector_clock: [],
      heartbeat_timeout: heartbeat_timeout
    }
  end
  @spec physical_node(%Dynamo{},any)::no_return()
  def physical_node(state,extra_state) do
    IO.puts("waiting for write")
  end

  @spec virtual_node(%Dynamo{},any)::no_return()
  def virtual_node(state,extra_state) do
    IO.puts("waiting for write")
  end
end

defmodule Dynamo.Client do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__
  defstruct(node_list: nil)

  @spec new_client(%{}) :: %Client{node_list: %{}}
  def new_client(node_list) do
    %Client{node_list: node_list}
  end
end
