defmodule ClusterLab.Memento.SimpleKV do
  use Memento.Table, attributes: [:key, :value]
  use ClusterLab.Memento.Common
end
