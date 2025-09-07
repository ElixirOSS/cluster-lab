defmodule ClusterLab.Topology do
  @moduledoc """
  Implements selective leader election using rendezvous hashing.

  Key features:
  - Only re-elects when actual leaders fail (not when non-leaders fail)
  - Uses :continue for non-blocking initialization
  - Event-driven elections with proper error handling
  - Supports adding/removing resources dynamically
  - Provides comprehensive status information

  Usage:
      # Start with initial resources
      {:ok, _pid} = ClusterLab.Topology.start_link(["shard_1", "shard_2", "cache_manager"])

      # Check leadership
      ClusterLab.Topology.am_i_leader?("shard_1")

      # Add new resources
      ClusterLab.Topology.add_resource("new_service")
  """
  use GenServer
  require Logger

  @initialization_timeout 30_000
  @max_init_attempts 3
  @main_cluster_resource "the-boss"

  defstruct [
    :resources,
    # resource -> leader_node
    :leaders,
    # current known alive nodes
    :topology,
    # resources currently being re-elected
    :elections_in_progress,
    :initialization_complete,
    :initialization_attempts,
    # processes subscribed to leadership changes
    :subscribers
  ]

  #############################################################################
  ## Public API
  #############################################################################

  @doc """
  Starts the leader election GenServer with initial resources.

  Options:
    - `:name` - GenServer name (defaults to __MODULE__)
  """
  def start_link(resources, opts \\ [])

  def start_link([], opts) do
    start_link([@main_cluster_resource], opts)
  end

  def start_link(resources, opts) when is_list(resources) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, resources, name: name)
  end

  @doc """
  Checks if the current node is the leader for a given resource.
  Returns `false` if the system is still initializing.
  """
  @spec am_i_leader?(String.t()) :: boolean()
  def am_i_leader?(resource) when is_binary(resource) do
    case GenServer.call(__MODULE__, {:am_i_leader, resource}) do
      {:ok, result} -> result
      {:error, :not_initialized} -> false
    end
  end

  @doc """
  Gets the current leader node for a resource.
  Returns `{:error, :not_initialized}` if system is still starting up.
  """
  @spec get_leader(String.t()) :: {:ok, node()} | {:error, :not_initialized | :resource_not_found}
  def get_leader(resource) when is_binary(resource) do
    GenServer.call(__MODULE__, {:get_leader, resource})
  end

  @doc """
  Gets all current resource-to-leader mappings.
  """
  @spec get_all_leaders() :: {:ok, %{String.t() => node()}} | {:error, :not_initialized}
  def get_all_leaders do
    GenServer.call(__MODULE__, :get_all_leaders)
  end

  @doc """
  Adds a new resource to be managed by leader election.
  Immediately elects a leader for the new resource.
  """
  @spec add_resource(String.t()) :: :ok | {:error, :not_initialized | :resource_exists}
  def add_resource(resource) when is_binary(resource) do
    GenServer.call(__MODULE__, {:add_resource, resource})
  end

  @doc """
  Removes a resource from leader election management.
  """
  @spec remove_resource(String.t()) :: :ok | {:error, :not_initialized}
  def remove_resource(resource) when is_binary(resource) do
    GenServer.call(__MODULE__, {:remove_resource, resource})
  end

  @doc """
  Gets comprehensive status information about the election system.
  """
  @spec get_status() :: map()
  def get_status do
    GenServer.call(__MODULE__, :get_status)
  end

  @doc """
  Subscribes the calling process to leadership change notifications.

  Notifications sent:
    - `{:leadership_gained, resource, new_leader, old_leader}`
    - `{:leadership_lost, resource, old_leader, new_leader}`
    - `{:initialization_complete, leaders}`
  """
  @spec subscribe() :: :ok
  def subscribe do
    GenServer.call(__MODULE__, {:subscribe, self()})
  end

  @doc """
  Unsubscribes from leadership change notifications.
  """
  @spec unsubscribe() :: :ok
  def unsubscribe do
    GenServer.call(__MODULE__, {:unsubscribe, self()})
  end

  @doc """
  Forces re-election of all resources (useful for rebalancing after cluster changes).
  Only works after initialization is complete.
  """
  @spec rebalance() :: :ok | {:error, :not_initialized}
  def rebalance do
    GenServer.call(__MODULE__, :rebalance)
  end

  #############################################################################
  ## GenServer Implementation
  #############################################################################

  def init(resources) do
    # Validate input
    if not (is_list(resources) and Enum.all?(resources, &is_binary/1)) do
      {:stop, {:bad_args, "resources must be a list of strings"}}
    else
      # Monitor cluster topology changes
      :net_kernel.monitor_nodes(true)

      state = %__MODULE__{
        resources: MapSet.new(resources),
        leaders: %{},
        topology: [],
        elections_in_progress: MapSet.new(),
        initialization_complete: false,
        initialization_attempts: 0,
        subscribers: MapSet.new()
      }

      Logger.info("Starting leader election for resources: #{inspect(resources)}")

      # Apply stabilization delay before first election so all early nodeups are observed
      stabilization_ms = 5_000
      Logger.info("Applying #{stabilization_ms}ms stabilization delay before initial election")
      Process.send_after(self(), :run_initial_election, stabilization_ms)
      {:ok, state}
    end
  end

  # Initial election is triggered via :run_initial_election after stabilization delay
  def handle_info(:run_initial_election, state) do
    attempt = state.initialization_attempts + 1

    Logger.info(
      "Leader election initialization (after stabilization) attempt #{attempt}/#{@max_init_attempts}"
    )

    case try_initial_election(state.resources) do
      {:ok, topology, leaders} ->
        Logger.info("✅ Leader election initialized successfully!")
        Logger.info("Cluster topology: #{inspect(topology)}")
        Logger.info("Initial leaders: #{inspect(leaders)}")

        new_state = %{state | leaders: leaders, topology: topology, initialization_complete: true}

        # Notify subscribers
        notify_subscribers(new_state, {:initialization_complete, leaders})

        {:noreply, new_state}

      {:error, reason} when attempt < @max_init_attempts ->
        Logger.warning(
          "❌ Initialization failed: #{inspect(reason)}, retrying in #{attempt * 2}s..."
        )

        # Exponential backoff (no stabilization delay on retries)
        delay = attempt * 2000
        Process.send_after(self(), :retry_initialization, delay)

        {:noreply, %{state | initialization_attempts: attempt}}

      {:error, reason} ->
        Logger.error(
          "💥 Initialization failed after #{@max_init_attempts} attempts: #{inspect(reason)}"
        )

        Logger.error("Starting with empty leadership - manual intervention may be required")

        # Start with empty leadership but mark as initialized
        new_state = %{
          state
          | initialization_complete: true,
            leaders: %{},
            # At least include ourselves
            topology: [node()]
        }

        {:noreply, new_state}
    end
  end

  def handle_info(:retry_initialization, state) do
    Logger.info("Retrying leader election initialization now")
    Process.send_after(self(), :run_initial_election, 0)
    {:noreply, state}
  end

  #############################################################################
  ## Node Monitoring - The Core Logic
  #############################################################################

  def handle_info({:nodedown, failed_node}, state) do
    if state.initialization_complete do
      handle_node_failure(state, failed_node)
    else
      Logger.debug("Ignoring node failure during initialization: #{failed_node}")
      {:noreply, state}
    end
  end

  def handle_info({:nodeup, new_node}, state) do
    if state.initialization_complete do
      handle_node_addition(state, new_node)
    else
      Logger.debug("Ignoring node addition during initialization: #{new_node}")
      {:noreply, state}
    end
  end

  #############################################################################
  ## Client API Handlers
  #############################################################################

  def handle_call({:am_i_leader, resource}, _from, state) do
    if state.initialization_complete do
      leader = Map.get(state.leaders, resource)
      result = leader == node()
      {:reply, {:ok, result}, state}
    else
      {:reply, {:error, :not_initialized}, state}
    end
  end

  def handle_call({:get_leader, resource}, _from, state) do
    if state.initialization_complete do
      case Map.get(state.leaders, resource) do
        nil -> {:reply, {:error, :resource_not_found}, state}
        leader -> {:reply, {:ok, leader}, state}
      end
    else
      {:reply, {:error, :not_initialized}, state}
    end
  end

  def handle_call(:get_all_leaders, _from, state) do
    if state.initialization_complete do
      {:reply, {:ok, state.leaders}, state}
    else
      {:reply, {:error, :not_initialized}, state}
    end
  end

  def handle_call({:add_resource, resource}, _from, state) do
    if not state.initialization_complete do
      {:reply, {:error, :not_initialized}, state}
    else
      if MapSet.member?(state.resources, resource) do
        {:reply, {:error, :resource_exists}, state}
      else
        # Add resource and immediately elect leader
        new_resources = MapSet.put(state.resources, resource)
        leader = ClusterLab.RendezvousHash.select_leader(resource, state.topology)
        new_leaders = Map.put(state.leaders, resource, leader)

        new_state = %{state | resources: new_resources, leaders: new_leaders}

        Logger.info("Added resource '#{resource}' with leader: #{leader}")

        # Notify if we became the leader
        if leader == node() do
          notify_subscribers(new_state, {:leadership_gained, resource, leader, nil})
        end

        {:reply, :ok, new_state}
      end
    end
  end

  def handle_call({:remove_resource, resource}, _from, state) do
    if not state.initialization_complete do
      {:reply, {:error, :not_initialized}, state}
    else
      old_leader = Map.get(state.leaders, resource)
      new_resources = MapSet.delete(state.resources, resource)
      new_leaders = Map.delete(state.leaders, resource)

      new_state = %{state | resources: new_resources, leaders: new_leaders}

      Logger.info("Removed resource '#{resource}' (was led by #{old_leader})")

      # Notify if we lost leadership
      if old_leader == node() do
        notify_subscribers(new_state, {:leadership_lost, resource, old_leader, nil})
      end

      {:reply, :ok, new_state}
    end
  end

  def handle_call(:get_status, _from, state) do
    status = %{
      initialized: state.initialization_complete,
      initialization_attempts: state.initialization_attempts,
      resources_count: MapSet.size(state.resources),
      resources: MapSet.to_list(state.resources),
      leaders: state.leaders,
      topology: state.topology,
      topology_size: length(state.topology),
      my_resources: get_my_resources(state),
      elections_in_progress: MapSet.to_list(state.elections_in_progress),
      subscribers_count: MapSet.size(state.subscribers)
    }

    {:reply, status, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    # Clean up if subscriber dies
    Process.monitor(pid)
    new_subscribers = MapSet.put(state.subscribers, pid)
    {:reply, :ok, %{state | subscribers: new_subscribers}}
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    new_subscribers = MapSet.delete(state.subscribers, pid)
    {:reply, :ok, %{state | subscribers: new_subscribers}}
  end

  def handle_call(:rebalance, _from, state) do
    if not state.initialization_complete do
      {:reply, {:error, :not_initialized}, state}
    else
      Logger.info("Manual rebalance requested")

      # Re-elect all leaders with current topology
      new_leaders = elect_all_leaders(state.resources, state.topology)

      # Notify about all changes
      notify_leadership_changes(state, state.leaders, new_leaders)

      new_state = %{state | leaders: new_leaders}
      {:reply, :ok, new_state}
    end
  end

  # def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
  #  # Clean up dead subscribers
  #  new_subscribers = MapSet.delete(state.subscribers, pid)
  #  {:noreply, %{state | subscribers: new_subscribers}}
  # end

  #############################################################################
  ## Private Functions - Node Event Handling
  #############################################################################

  defp handle_node_failure(state, failed_node) do
    Logger.info("Node failed: #{failed_node}")

    # Find resources that were led by the failed node
    affected_resources =
      state.leaders
      |> Enum.filter(fn {_resource, leader} -> leader == failed_node end)
      |> Enum.map(fn {resource, _leader} -> resource end)

    # Update topology
    new_topology = List.delete(state.topology, failed_node)

    cond do
      length(affected_resources) > 0 ->
        # Failed node was leading something - immediate re-election needed
        Logger.warning(
          "💥 Leader failed! Re-electing for resources: #{inspect(affected_resources)}"
        )

        new_state = start_elections_for_resources(state, affected_resources, new_topology)
        {:noreply, %{new_state | topology: new_topology}}

      true ->
        # Failed node wasn't leading anything - just update topology
        Logger.info("ℹ️  Non-leader failed, no re-election needed")
        {:noreply, %{state | topology: new_topology}}
    end
  end

  defp handle_node_addition(state, new_node) do
    Logger.info("Node joined cluster: #{new_node}")

    new_topology =
      [new_node | state.topology]
      |> Enum.uniq()
      |> Enum.sort()

    # Determine which resources would change leader given the new topology
    changed_resources =
      state.resources
      |> Enum.filter(fn resource ->
        old_leader = Map.get(state.leaders, resource)
        new_leader = ClusterLab.RendezvousHash.select_leader(resource, new_topology)
        new_leader != old_leader
      end)

    case changed_resources do
      [] ->
        Logger.info("Topology updated; no leadership changes required")
        {:noreply, %{state | topology: new_topology}}

      resources ->
        Logger.info(
          "Topology updated; re-electing leaders for impacted resources: #{inspect(resources)}"
        )

        new_state =
          start_elections_for_resources(
            %{state | topology: new_topology},
            resources,
            new_topology
          )

        {:noreply, %{new_state | topology: new_topology}}
    end
  end

  #############################################################################
  ## Private Functions - Election Logic
  #############################################################################

  defp try_initial_election(resources) do
    try do
      task =
        Task.async(fn ->
          topology = discover_cluster_topology()

          if length(topology) == 0 do
            throw({:error, :no_nodes_available})
          end

          leaders = elect_all_leaders(resources, topology)
          {:ok, topology, leaders}
        end)

      Task.await(task, @initialization_timeout)
    rescue
      e -> {:error, {:exception, Exception.message(e)}}
    catch
      :exit, {:timeout, _} ->
        {:error, :timeout}

      :throw, error ->
        error
    end
  end

  defp discover_cluster_topology do
    # Get currently connected nodes
    base_nodes = [node() | Node.list()]

    # Could be extended to do additional discovery:
    # - DNS SRV lookups
    # - Consul/etcd service discovery
    # - Configuration-based node lists
    # - Health checks

    additional_nodes = discover_additional_nodes()

    (base_nodes ++ additional_nodes)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp discover_additional_nodes do
    # Placeholder for additional node discovery
    # In production this might query external services
    []
  end

  defp start_elections_for_resources(state, resources, new_topology) do
    # Mark resources as having elections in progress
    new_elections_in_progress =
      resources
      |> Enum.reduce(state.elections_in_progress, &MapSet.put(&2, &1))

    # Elect new leaders for affected resources only
    new_leaders_for_resources = elect_specific_leaders(resources, new_topology)

    # Update leaders map - remove old, add new
    new_leaders =
      state.leaders
      # Remove old leaders for affected resources
      |> Map.drop(resources)
      # Add new leaders
      |> Map.merge(new_leaders_for_resources)

    # Notify about leadership changes
    notify_leadership_changes(state, state.leaders, new_leaders)

    # Clear election-in-progress flags
    new_elections_cleared =
      resources
      |> Enum.reduce(new_elections_in_progress, &MapSet.delete(&2, &1))

    %{state | leaders: new_leaders, elections_in_progress: new_elections_cleared}
  end

  defp elect_all_leaders(resources, topology) do
    resources
    |> Enum.reduce(%{}, fn resource, acc ->
      leader = ClusterLab.RendezvousHash.select_leader(resource, topology)
      Map.put(acc, resource, leader)
    end)
  end

  defp elect_specific_leaders(resources, topology) do
    resources
    |> Enum.reduce(%{}, fn resource, acc ->
      leader = ClusterLab.RendezvousHash.select_leader(resource, topology)
      Map.put(acc, resource, leader)
    end)
  end

  #############################################################################
  ## Private Functions - Notifications
  #############################################################################

  defp notify_leadership_changes(state, old_leaders, new_leaders) do
    # Find what changed
    changes = find_leadership_changes(old_leaders, new_leaders)

    # Log changes
    Enum.each(changes, fn
      {:gained, resource, new_leader, old_leader} ->
        if new_leader == node() do
          Logger.info("🎉 Gained leadership for: #{resource} (was #{old_leader || "none"})")
        end

      {:lost, resource, old_leader, new_leader} ->
        if old_leader == node() do
          Logger.info("📤 Lost leadership for: #{resource} (now #{new_leader})")
        end
    end)

    # Notify subscribers
    Enum.each(changes, fn change ->
      case change do
        {:gained, resource, new_leader, old_leader} ->
          notify_subscribers(state, {:leadership_gained, resource, new_leader, old_leader})

        {:lost, resource, old_leader, new_leader} ->
          notify_subscribers(state, {:leadership_lost, resource, old_leader, new_leader})
      end
    end)
  end

  defp find_leadership_changes(old_leaders, new_leaders) do
    all_resources =
      MapSet.union(
        MapSet.new(Map.keys(old_leaders)),
        MapSet.new(Map.keys(new_leaders))
      )

    all_resources
    |> Enum.flat_map(fn resource ->
      old_leader = Map.get(old_leaders, resource)
      new_leader = Map.get(new_leaders, resource)

      cond do
        old_leader == new_leader ->
          # No change
          []

        old_leader == nil ->
          [{:gained, resource, new_leader, old_leader}]

        new_leader == nil ->
          [{:lost, resource, old_leader, new_leader}]

        true ->
          [{:lost, resource, old_leader, new_leader}, {:gained, resource, new_leader, old_leader}]
      end
    end)
  end

  defp notify_subscribers(state, message) do
    Enum.each(state.subscribers, fn subscriber ->
      try do
        send(subscriber, message)
      catch
        :error, :badarg ->
          # Subscriber process is dead, will be cleaned up by monitor
          :ok
      end
    end)
  end

  defp get_my_resources(state) do
    state.leaders
    |> Enum.filter(fn {_resource, leader} -> leader == node() end)
    |> Enum.map(fn {resource, _leader} -> resource end)
  end
end
