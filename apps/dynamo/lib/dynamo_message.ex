defmodule Dynamo.ObjectEntry do
  @moduledoc """
  Object entry for Dyname, contain object_value, object_version(vector_clock), is_replica, hash_code
  """
  alias Dynamo.ObjectEntry
  alias __MODULE__
  @enforce_keys [:value, :value_vector_clock, :is_replica, :hash_code]
  defstruct(
    value: nil,
    value_vector_clock: nil,
    is_replica: nil,
    hash_code: nil,
  )
  @doc """
  Return a ObjecyEntry for putting in to the hash_table
  """
  @spec putObject(non_neg_integer(), %{}, non_neg_integer(), non_neg_integer()):: %ObjectEntry{
    value: non_neg_integer(),
    value_vector_clock: %{},
    is_replica: non_neg_integer(),
    hash_code: non_neg_integer(),
  }
  def putObject(value, value_vector_clock, is_replica, hash_code) do
    %ObjectEntry{
      value: value,
      value_vector_clock: value_vector_clock,
      is_replica: is_replica,
      hash_code: hash_code
    }
  end
end


defmodule Dynamo.PutEntryRequest do
  @moduledoc """
  Put(key,context,object) RPC request
  coordinate node send this RPC to replica node
  """
  alias __MODULE__
  @enforce_keys [:key,
    :value,
    :hash_code,
    :value_vector_clock
  ]

  defstruct(
    key: nil,
    value: nil,
    hash_code: nil,
    value_vector_clock: nil
  )
  @doc """
  Create a new PutEntryRequest
  """
  @spec new(
    non_neg_integer(),
    non_neg_integer(),
    non_neg_integer(),
    map()
  ):: %PutEntryRequest{
    key: non_neg_integer(),
    value: non_neg_integer(),
    hash_code: non_neg_integer(),
    value_vector_clock: map()
  }
  def new(key, value, hash_code, value_vector_clock) do
    %PutEntryRequest{
      key: key,
      value: value,
      hash_code: hash_code,
      value_vector_clock: value_vector_clock
    }
  end
end

defmodule Dynamo.PutEntryResponse do
  @moduledoc """
  Response for the PutEntryRequest
  """
  alias __MODULE__
  @enforce_keys [:key_value, :success]
  defstruct(
    key_value: nil,
    success: nil
  )

  @doc """
  Create a new PutEntryResponse
  """
alias Dynamo.PutEntryResponse
  @spec new(non_neg_integer(),boolean()):: %PutEntryResponse{
    key_value: non_neg_integer(),
    success: boolean()
  }
  def new(key_value, success) do
    %PutEntryResponse{
      key_value: key_value,
      success: success
    }
  end
end

defmodule Dynamo.GetEntryRequest do
  @moduledoc """
  GetEntry RPC request, coordinate node use this RPC to call replica node
  """
  alias __MODULE__
  @enforce_keys [:key, :hash_tree_root]
  defstruct(
    key: nil,
    hash_tree_root: nil
  )

alias Dynamo.GetEntryRequest
  @spec new(non_neg_integer(), any()) :: %GetEntryRequest{
    key: non_neg_integer(),
    hash_tree_root: any()
  }
  def new(key, hash_tree) do
    %GetEntryRequest{
      key: key,
      hash_tree_root: hash_tree
    }
  end
end

defmodule Dynamo.GetEntryResponse do
  @moduledoc """
  Response for GetEntryRequest
  """
  alias Dynamo.GetEntryResponse
  alias __MODULE__
  @enforce_keys [:key, :value, :is_same, :value_vector_clock]
  defstruct(
    key: nil,
    value: nil,
    is_same: nil,
    value_vector_clock: nil
  )

  @doc """
  Create a new GetEntryResponse
  """
  @spec new(non_neg_integer(), any(), boolean(), map())::
  %GetEntryResponse{
    key: non_neg_integer(),
    value: non_neg_integer(),
    is_same: boolean(),
    value_vector_clock: map()
  }
  def new(key, value, is_same, value_vector_clock) do
    %GetEntryResponse{
      key: key,
      value: value,
      is_same: is_same,
      value_vector_clock: value_vector_clock
    }
  end
end

defmodule Dynamo.UpdateHashTableRequest do
  alias __MODULE__
  @enforce_keys [:key, :value, :value_vector_clock]
  defstruct(
    key: nil,
    value: nil,
    hash_code: nil,
    value_vector_clock: nil
  )

  @spec new(non_neg_integer(),non_neg_integer(),non_neg_integer(), map())::
  %UpdateHashTableRequest{
    key: non_neg_integer(),
    value: non_neg_integer(),
    hash_code: non_neg_integer(),
    value_vector_clock: map()
  }
  def new(key, value, hash_code, value_vector_clock) do
    %UpdateHashTableRequest{
      key: key,
      value: value,
      hash_code: hash_code,
      value_vector_clock: value_vector_clock
    }
  end
end
