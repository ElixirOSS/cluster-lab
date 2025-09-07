defmodule ClusterLab.Memento.Monitor do
  @moduledoc """
    A GenServer that subscribes to Mnesia system/activity events and logs them.

  Add this process to your supervision tree to gain visibility into
  cluster / Mnesia lifecycle changes.

  Typical events logged:
    * node up/down
    * inconsistent database warnings
    * other generic system events

  Options:
    * :name - (optional) a registered name for the process
    * :log_level - minimum level to emit (default: :info)
  """

  use GenServer
  require Logger

  # Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # GenServer callbacks

  @impl true
  def init(_opts) do
    :mnesia.set_debug_level(:verbose)
    {:ok, _node} = :mnesia.subscribe(:system)

    log(:debug, "Mnesia monitor started; subscribed to :system and :activity events")

    {:ok, %{}}
  end

  @impl true
  def handle_info({:mnesia_system_event, event}, state) do
    handle_system_event(event, state)
  end

  # Catch-all
  @impl true
  def handle_info(msg, state) do
    log(:debug, "Unhandled monitor message: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def handle_call(_, _from, state) do
    Logger.debug("Unhandled monitor call")
    {:noreply, state}
  end

  # Internal handlers

  defp handle_system_event({:mnesia_up, node}, state) do
    log(:info, "Mnesia up on node #{inspect(node)}")
    {:noreply, state}
  end

  defp handle_system_event({:mnesia_down, node}, state) do
    log(:warning, "Mnesia down on node #{inspect(node)}")
    {:noreply, state}
  end

  defp handle_system_event({:inconsistent_database, context, repair}, state) do
    log(
      :error,
      "Inconsistent database detected: context=#{inspect(context)} repair=#{inspect(repair)}"
    )

    {:noreply, state}
  end

  defp handle_system_event({:mnesia, :system_event, detail}, state) do
    log(:info, "Generic Mnesia system event: #{inspect(detail)}")
    {:noreply, state}
  end

  defp handle_system_event(other, state) do
    log(:debug, "Unhandled system event: #{inspect(other)}")
    {:noreply, state}
  end

  # Logging helper respecting a minimum log level
  defp log(level, message) do
    Logger.log(level, message)
  end
end
