defmodule RaftTest do
  use ExUnit.Case
  doctest Raft
  @spec hash_function(String.t()) :: String.t()
  def hash_function(meta_data) do
    result = MerkleTree.Crypto.hash(meta_data,:md5)
    result
  end
  @spec calculate_pos(String.t())::non_neg_integer()
  def calculate_pos(hash_code) do
    result = rem(:binary.decode_unsigned(hash_code),365)
    result
  end
  test "Hash Tree used correct" do
    result= MerkleTree.Crypto.hash("tendermint", :md5)
    number_result = calculate_pos(result)
    IO.puts(number_result)
    assert MerkleTree.Crypto.hash("tendermint", :md5) == "bc93700bdf1d47ad28654ad93611941f"
  end

  test "Build hash tree with own hash function" do
    mt = MerkleTree.new(["a", "b", "c", "d"],&hash_function/1)
    IO.inspect(mt)
  end
end
