defmodule ClusterLab.RendezvousHash do
  @moduledoc """
  Rendezvous hashing implementation for deterministic leader selection.

  Uses erlang's phash2 for speed and native term support.
  Every node will independently compute the same leader for any resource.

  If you haven't seen Bryan Hunter's presentation on Waterpark, you should
  really [check it out](https://youtu.be/hdBm4K-vvt0?si=7IU2vLBixi7WiAeu)
  """

  @doc """
  Selects the leader node for a given resource using rendezvous hashing.

  ## Examples

      iex> ClusterLab.RendezvousHash.select_leader("cache", [:node1, :node2, :node3])
      :node2

      iex> ClusterLab.RendezvousHash.select_leader("cache", [:node1, :node3])
      :node1  # Deterministic failover
  """
  @spec select_leader(String.t(), [node()]) :: node()
  def select_leader(resource, nodes)
      when is_binary(resource) and is_list(nodes) and length(nodes) > 0 do
    nodes
    |> Enum.map(&hash(resource, &1))
    |> Enum.max_by(fn {_node, weight} -> weight end)
    |> elem(0)
  end

  @doc """
  Selects the top N nodes for a resource (useful for replicas/backups).

  ## Examples

      iex> ClusterLab.RendezvousHash.select_top_nodes("data", [:node1, :node2, :node3], 2)
      [:node2, :node1]  # Primary and backup
  """
  @spec select_top_nodes(String.t(), [node()], pos_integer()) :: [node()]
  def select_top_nodes(resource, nodes, n)
      when is_binary(resource) and is_list(nodes) and n > 0 and n <= length(nodes) do
    nodes
    |> Enum.map(&hash(resource, &1))
    |> Enum.sort_by(fn {_node, weight} -> weight end, :desc)
    |> Enum.take(n)
    |> Enum.map(fn {node, _weight} -> node end)
  end

  defp hash(resource, node) do
    {node, :erlang.phash2({resource, node})}
  end
end
