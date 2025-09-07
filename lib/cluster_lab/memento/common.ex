defmodule ClusterLab.Memento.Common do
  defmacro __using__(_) do
    quote do
      def current_size_bytes() do
        Memento.Table.info(__MODULE__, :memory) * :erlang.system_info(:wordsize)
      end

      def all do
        Memento.transaction!(fn ->
          Memento.Query.all(__MODULE__)
        end)
      end

      def get(key) do
        Memento.transaction!(fn ->
          Memento.Query.read(__MODULE__, key)
        end)
      end

      def put(key, value) do
        Memento.transaction!(fn ->
          Memento.Query.write(%__MODULE__{key: key, value: value})
        end)
      end

      def first do
        Memento.transaction!(fn ->
          case :mnesia.first(__MODULE__) do
            :"$end_of_table" ->
              nil

            key ->
              Memento.Query.read(__MODULE__, key)
          end
        end)
      end

      def last do
        Memento.transaction!(fn ->
          case :mnesia.last(__MODULE__) do
            :"$end_of_table" ->
              nil

            key ->
              Memento.Query.read(__MODULE__, key)
          end
        end)
      end
    end
  end
end
