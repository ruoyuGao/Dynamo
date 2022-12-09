defmodule RaftTest do
  use ExUnit.Case
  doctest Raft
  test "Hash Tree used correct" do
    assert MerkleTree.Crypto.hash("tendermint", :md5) == "bc93700bdf1d47ad28654ad93611941f"
  end
end
